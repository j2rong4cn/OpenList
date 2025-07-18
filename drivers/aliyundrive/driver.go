package aliyundrive

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"os"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/cron"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type AliDrive struct {
	model.Storage
	Addition
	AccessToken string
	cron        *cron.Cron
	DriveId     string
	UserID      string
	ref         *AliDrive
}

func (d *AliDrive) Config() driver.Config {
	return config
}

func (d *AliDrive) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *AliDrive) Init(ctx context.Context) error {
	// TODO login / refresh token
	//op.MustSaveDriverStorage(d)
	if d.ref == nil {
		err := d.refreshToken()
		if err != nil {
			return err
		}
		// get driver id
		res, err, _ := d.request("https://api.alipan.com/v2/user/get", http.MethodPost, nil, nil)
		if err != nil {
			return err
		}
		d.UserID = utils.Json.Get(res, "user_id").ToString()
		d.cron = cron.NewCron(time.Hour * 2)
		d.cron.Do(func() {
			err := d.refreshToken()
			if err != nil {
				log.Errorf("%+v", err)
			}
		})
	}
	if d.ref != nil || global.Has(d.UserID) {
		err := d.refreshDriveId()
		return err
	}
	// init deviceID
	deviceID := utils.HashData(utils.SHA256, []byte(d.UserID))
	// init privateKey
	privateKey, _ := NewPrivateKeyFromHex(deviceID)
	state := State{
		privateKey: privateKey,
		deviceID:   deviceID,
	}
	// store state
	global.Store(d.UserID, &state)
	// init signature
	d.sign()
	err := d.refreshDriveId()
	return err
}

func (d *AliDrive) InitReference(storage driver.Driver) error {
	refStorage, ok := storage.(*AliDrive)
	if ok {
		d.ref = refStorage
		return nil
	}
	return errs.NotSupport
}

func (d *AliDrive) Drop(ctx context.Context) error {
	if d.cron != nil {
		d.cron.Stop()
	}
	return nil
}

func (d *AliDrive) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	parentId := ""
	if dir.GetID() == "root" {
		parentId = "root"
	} else {
		parentId = dir.GetID()
	}

	files, err := d.getFiles(parentId)
	if err != nil {
		return nil, err
	}
	return utils.SliceConvert(files, func(src File) (model.Obj, error) {
		return fileToObj(src), nil
	})
}

func (d *AliDrive) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	data := base.Json{
		"drive_id":   d.DriveId,
		"file_id":    file.GetID(),
		"expire_sec": 14400,
	}
	res, err, _ := d.requestS("https://api.alipan.com/v2/file/get_download_url", http.MethodPost, data, nil, nil)
	if err != nil {
		return nil, err
	}
	url := utils.Json.Get(res, "url").ToString()
	if len(url) == 0 {
		// 从 Flask后端解密 URL
		url, _ = d.decryptURL(utils.Json.Get(res, "encrypt_url").ToString())
	}
	exp := time.Minute
	return &model.Link{
		Header: http.Header{
			"Referer":    []string{"https://alipan.com/"},
			"User-Agent": []string{d.Addition.UserAgent},
		},
		URL:        url,
		Expiration: &exp,
	}, nil
}

func (d *AliDrive) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	_, err, _ := d.requestS("https://api.alipan.com/adrive/v2/file/createWithFolders", http.MethodPost, base.Json{
		"check_name_mode": "refuse",
		"drive_id":        d.DriveId,
		"name":            dirName,
		"parent_file_id":  parentDir.GetID(),
		"type":            "folder",
	}, nil, nil)
	return err
}

func (d *AliDrive) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	err := d.batch(srcObj.GetID(), dstDir.GetID(), "/file/move")
	return err
}

func (d *AliDrive) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	_, err, _ := d.requestS("https://api.alipan.com/v3/file/update", http.MethodPost, base.Json{
		"check_name_mode": "refuse",
		"drive_id":        d.DriveId,
		"file_id":         srcObj.GetID(),
		"name":            newName,
	}, nil, nil)
	return err
}

func (d *AliDrive) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	err := d.batch(srcObj.GetID(), dstDir.GetID(), "/file/copy")
	return err
}

func (d *AliDrive) Remove(ctx context.Context, obj model.Obj) error {
	_, err, _ := d.requestS("https://api.alipan.com/v2/recyclebin/trash", http.MethodPost, base.Json{
		"drive_id": d.DriveId,
		"file_id":  obj.GetID(),
	}, nil, nil)
	return err
}

func (d *AliDrive) Put(ctx context.Context, dstDir model.Obj, streamer model.FileStreamer, up driver.UpdateProgress) error {
	file := &stream.FileStream{
		Obj:      streamer,
		Reader:   streamer,
		Mimetype: streamer.GetMimetype(),
	}
	const DEFAULT int64 = 10485760
	var count = int(math.Ceil(float64(streamer.GetSize()) / float64(DEFAULT)))

	partInfoList := make([]base.Json, 0, count)
	for i := 1; i <= count; i++ {
		partInfoList = append(partInfoList, base.Json{"part_number": i})
	}
	reqBody := base.Json{
		"check_name_mode": "overwrite",
		"drive_id":        d.DriveId,
		"name":            file.GetName(),
		"parent_file_id":  dstDir.GetID(),
		"part_info_list":  partInfoList,
		"size":            file.GetSize(),
		"type":            "file",
	}

	var localFile *os.File
	if fileStream, ok := file.Reader.(*stream.FileStream); ok {
		localFile, _ = fileStream.Reader.(*os.File)
	}
	if d.RapidUpload {
		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		utils.CopyWithBufferN(buf, file, 1024)
		reqBody["pre_hash"] = utils.HashData(utils.SHA1, buf.Bytes())
		if localFile != nil {
			if _, err := localFile.Seek(0, io.SeekStart); err != nil {
				return err
			}
		} else {
			// 把头部拼接回去
			file.Reader = struct {
				io.Reader
				io.Closer
			}{
				Reader: io.MultiReader(buf, file),
				Closer: file,
			}
		}
	} else {
		reqBody["content_hash_name"] = "none"
		reqBody["proof_version"] = "v1"
	}

	var resp UploadResp
	_, err, e := d.requestS("https://api.alipan.com/adrive/v2/file/createWithFolders", http.MethodPost, reqBody, nil, &resp)

	if err != nil && e.Code != "PreHashMatched" {
		return err
	}

	if d.RapidUpload && e.Code == "PreHashMatched" {
		delete(reqBody, "pre_hash")
		h := sha1.New()
		if localFile != nil {
			if err = utils.CopyWithCtx(ctx, h, localFile, 0, nil); err != nil {
				return err
			}
			if _, err = localFile.Seek(0, io.SeekStart); err != nil {
				return err
			}
		} else {
			tempFile, err := os.CreateTemp(conf.Conf.TempDir, "file-*")
			if err != nil {
				return err
			}
			defer func() {
				_ = tempFile.Close()
				_ = os.Remove(tempFile.Name())
			}()
			if err = utils.CopyWithCtx(ctx, io.MultiWriter(tempFile, h), file, 0, nil); err != nil {
				return err
			}
			localFile = tempFile
		}
		reqBody["content_hash"] = hex.EncodeToString(h.Sum(nil))
		reqBody["content_hash_name"] = "sha1"
		reqBody["proof_version"] = "v1"

		/*
			js 隐性转换太坑不知道有没有bug
			var n = e.access_token，
			r = new BigNumber('0x'.concat(md5(n).slice(0, 16)))，
			i = new BigNumber(t.file.size)，
			o = i ? r.mod(i) : new gt.BigNumber(0);
			(t.file.slice(o.toNumber(), Math.min(o.plus(8).toNumber(), t.file.size)))
		*/
		buf := make([]byte, 8)
		r, _ := new(big.Int).SetString(utils.GetMD5EncodeStr(d.getAccessToken())[:16], 16)
		i := new(big.Int).SetInt64(file.GetSize())
		o := new(big.Int).SetInt64(0)
		if file.GetSize() > 0 {
			o = r.Mod(r, i)
		}
		n, _ := io.NewSectionReader(localFile, o.Int64(), 8).Read(buf[:8])
		reqBody["proof_code"] = base64.StdEncoding.EncodeToString(buf[:n])

		_, err, e := d.requestS("https://api.alipan.com/adrive/v2/file/createWithFolders", http.MethodPost, reqBody, nil, &resp)
		if err != nil && e.Code != "PreHashMatched" {
			return err
		}
		if resp.RapidUpload {
			return nil
		}
		// 秒传失败
		if _, err = localFile.Seek(0, io.SeekStart); err != nil {
			return err
		}
		file.Reader = localFile
	}

	for i, partInfo := range resp.PartInfoList {
		if utils.IsCanceled(ctx) {
			return ctx.Err()
		}
		url := partInfo.UploadUrl
		if d.InternalUpload {
			url = partInfo.InternalUploadUrl
		}
		req, err := http.NewRequest("PUT", url, io.LimitReader(file, DEFAULT))
		if err != nil {
			return err
		}
		req = req.WithContext(ctx)
		res, err := base.HttpClient.Do(req)
		if err != nil {
			return err
		}
		res.Body.Close()
		if count > 0 {
			up(float64(i) * 100 / float64(count))
		}
	}
	var resp2 base.Json
	_, err, e = d.requestS("https://api.alipan.com/v2/file/complete", http.MethodPost, base.Json{
		"drive_id":  d.DriveId,
		"file_id":   resp.FileId,
		"upload_id": resp.UploadId,
	}, nil, &resp2)
	if err != nil && e.Code != "PreHashMatched" {
		return err
	}
	if resp2["file_id"] == resp.FileId {
		return nil
	}
	return fmt.Errorf("%+v", resp2)
}

func (d *AliDrive) Other(ctx context.Context, args model.OtherArgs) (interface{}, error) {
	var resp base.Json
	var url string
	data := base.Json{
		"drive_id": d.DriveId,
		"file_id":  args.Obj.GetID(),
	}
	switch args.Method {
	case "doc_preview":
		url = "https://api.alipan.com/v2/file/get_office_preview_url"
		data["access_token"] = d.getAccessToken()
	case "video_preview":
		url = "https://api.alipan.com/v2/file/get_video_preview_play_info"
		data["category"] = "live_transcoding"
		data["url_expire_sec"] = 14400
		data["get_subtitle_info"] = true
		data["mode"] = "high_res"
		data["template_id"] = ""
	default:
		return nil, errs.NotSupport
	}
	_, err, _ := d.requestS(url, http.MethodPost, data, nil, &resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

var _ driver.Driver = (*AliDrive)(nil)
