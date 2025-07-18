package aliyundrive

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/dustinxie/ecc"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
)

func (d *AliDrive) createSession() error {
	state, ok := global.Load(d.UserID)
	if !ok {
		return fmt.Errorf("can't load user state, user_id: %s", d.UserID)
	}
	d.sign()
	state.retry++
	if state.retry > 3 {
		state.retry = 0
		return fmt.Errorf("createSession failed after three retries")
	}
	_, err, _ := d.requestS("https://api.alipan.com/users/v1/users/device/create_session", http.MethodPost, base.Json{
		"deviceName":   "samsung",
		"modelName":    "SM-G9810",
		"nonce":        0,
		"pubKey":       PublicKeyToHex(&state.privateKey.PublicKey),
		"refreshToken": d.RefreshToken,
	}, nil, nil)
	if err == nil {
		state.retry = 0
	}
	return err
}

// func (d *AliDrive) renewSession() error {
// 	_, err, _ := d.requestS("https://api.alipan.com/users/v1/users/device/renew_session", http.MethodPost, nil, nil)
// 	return err
// }

func (d *AliDrive) sign() {
	state, _ := global.Load(d.UserID)
	secpAppID := "5dde4e1bdf9e4966b387ba58f4b3fdc3"
	singdata := fmt.Sprintf("%s:%s:%s:%d", secpAppID, state.deviceID, d.UserID, 0)
	hash := sha256.Sum256([]byte(singdata))
	data, _ := ecc.SignBytes(state.privateKey, hash[:], ecc.RecID|ecc.LowerS)
	state.signature = hex.EncodeToString(data) //strconv.Itoa(state.nonce)
}

// do others that not defined in Driver interface

func (d *AliDrive) refreshToken() error {
	// if d.ref != nil {
	// 	return d.ref.refreshToken()
	// }
	url := "https://auth.alipan.com/v2/account/token"
	var resp base.TokenResp
	var e RespErr
	_, err := base.RestyClient.R().
		//ForceContentType("application/json").
		SetBody(base.Json{"refresh_token": d.RefreshToken, "grant_type": "refresh_token"}).
		SetResult(&resp).
		SetError(&e).
		Post(url)
	if err != nil {
		return err
	}
	if e.Code != "" {
		return fmt.Errorf("failed to refresh token: %s", e.Message)
	}
	if resp.RefreshToken == "" {
		return errors.New("failed to refresh token: refresh token is empty")
	}
	d.RefreshToken, d.AccessToken = resp.RefreshToken, resp.AccessToken
	op.MustSaveDriverStorage(d)
	return nil
}

func (d *AliDrive) request(url, method string, callback base.ReqCallback, resp interface{}) ([]byte, error, RespErr) {
	if d.ref != nil {
		return d.ref.request(url, method, callback, resp)
	}
	req := base.RestyClient.R()
	state, ok := global.Load(d.UserID)
	if !ok {
		if url == "https://api.alipan.com/v2/user/get" {
			state = &State{}
		} else {
			return nil, fmt.Errorf("can't load user state, user_id: %s", d.UserID), RespErr{}
		}
	}
	req.SetHeaders(map[string]string{
		//"Authorization": "Bearer\t" + d.AccessToken,
		"Authorization": d.AccessToken,
		"content-type":  "application/json; charset=UTF-8",
		"origin":        "https://www.alipan.com",
		"Referer":       "https://alipan.com/",
		"X-Signature":   state.signature,
		"x-request-id":  uuid.NewString(),
		"X-Canary":      d.XCanary,
		"X-Device-Id":   state.deviceID,
	})
	if callback != nil {
		callback(req)
	} else {
		req.SetBody("{}")
	}
	if resp != nil {
		req.SetResult(resp)
	}
	var e RespErr
	req.SetError(&e)
	res, err := req.Execute(method, url)
	if err != nil {
		return nil, err, e
	}
	if e.Code != "" {
		switch e.Code {
		case "AccessTokenInvalid":
			err = d.refreshToken()
			if err != nil {
				return nil, err, e
			}
		case "DeviceSessionSignatureInvalid":
			err = d.createSession()
			if err != nil {
				return nil, err, e
			}
		default:
			return nil, errors.New(e.Message), e
		}
		return d.request(url, method, callback, resp)
	} else if res.IsError() {
		return nil, errors.New("bad status code " + res.Status()), e
	}
	return res.Body(), nil, e
}

func (d *AliDrive) requestS(url, method string, data interface{}, headers map[string]string, resp interface{}) ([]byte, error, RespErr) {
	if d.ref != nil {
		return d.ref.requestS(url, method, data, headers, resp)
	}
	timestamp := fmt.Sprint(time.Now().UnixMilli())
	nonce := uuid.NewString()
	var e RespErr
	signV2, err := d.getSign(timestamp, nonce)
	if err != nil {
		return nil, err, e
	}

	reqHeaders := map[string]string{
		"x-signature-v2": signV2,
		"x-nonce":        nonce,
		"x-timestamp":    timestamp,
		"User-Agent":     d.UserAgent,
	}

	// Merge additional headers if provided
	for k, v := range headers {
		reqHeaders[k] = v
	}
	// 获取 x-mini-wua x-sgext x-sign x-umt x-bx-version
	bJson, err := d.generateSecurityHeader(url)
	if err != nil {
		return nil, err, e
	}

	for k, v := range bJson {
		reqHeaders[k] = v.(string)
	}

	res, err, e := d.request(url, method, func(req *resty.Request) {
		req.SetHeaders(reqHeaders)
		req.SetBody(data)
	}, resp)

	return res, err, e
}

func (d *AliDrive) getFiles(fileId string) ([]File, error) {
	marker := "first"
	res := make([]File, 0)
	for marker != "" {
		if marker == "first" {
			marker = ""
		}
		var resp Files
		data := base.Json{
			"all":                     false,
			"enable_timeline":         false,
			"drive_id":                d.DriveId,
			"fields":                  "*",
			"image_thumbnail_process": "image/resize,m_lfit,w_256,limit_0/format,avif",
			"image_url_process":       "image/resize,m_lfit,w_1080/format,avif",
			"limit":                   100,
			"marker":                  marker,
			"order_by":                d.OrderBy,
			"order_direction":         d.OrderDirection,
			"parent_file_id":          fileId,
			"timeline_order_by":       "name asc",
			"video_thumbnail_process": "video/snapshot,t_120000,f_jpg,m_lfit,w_256,ar_auto,m_fast",
		}
		_, err, _ := d.requestS("https://api.alipan.com/adrive/v4/file/list", http.MethodPost, data, nil, &resp)

		if err != nil {
			return nil, err
		}
		marker = resp.NextMarker
		res = append(res, resp.Items...)
	}
	return res, nil
}

func (d *AliDrive) batch(srcId, dstId string, url string) error {
	res, err, _ := d.requestS("https://api.alipan.com/v3/batch", http.MethodPost, base.Json{
		"requests": []base.Json{
			{
				"headers": base.Json{
					"Content-Type": "application/json",
				},
				"method": "POST",
				"id":     srcId,
				"body": base.Json{
					"drive_id":          d.DriveId,
					"file_id":           srcId,
					"to_drive_id":       d.DriveId,
					"to_parent_file_id": dstId,
				},
				"url": url,
			},
		},
		"resource": "file",
	}, nil, nil)
	if err != nil {
		return err
	}
	status := utils.Json.Get(res, "responses", 0, "status").ToInt()
	if status < 400 && status >= 100 {
		return nil
	}
	return errors.New(string(res))
}

func (d *AliDrive) getSign(timestamp, nonce string) (string, error) {
	// if d.ref != nil {
	// 	return d.ref.getSign(timestamp, nonce)
	// }
	req := base.RestyClient.R()
	state, _ := global.Load(d.UserID)
	data := base.Json{
		"device_id": state.deviceID,
		"timestamp": timestamp,
		"uuid":      nonce,
	}
	req.SetBody(data)
	res, err := req.Execute(http.MethodPost, d.HookAddress+"/encrypt")
	if err != nil {
		return "", err
	}
	return string(res.Body()), nil
}

func (d *AliDrive) decryptURL(encryptURL string) (string, error) {
	if d.ref != nil {
		return d.ref.decryptURL(encryptURL)
	}
	req := base.RestyClient.R()
	data := base.Json{
		"encrypt_url": encryptURL,
	}
	req.SetBody(data)
	res, err := req.Execute(http.MethodPost, d.HookAddress+"/decrypt")
	if err != nil {
		return "", err
	}
	return string(res.Body()), nil
}

func (d *AliDrive) generateSecurityHeader(url string) (base.Json, error) {
	// if d.ref != nil {
	// 	return d.ref.generateSecurityHeader(url)
	// }
	req := base.RestyClient.R()
	data := base.Json{
		"url": url,
	}
	req.SetBody(data)
	res, err := req.Execute(http.MethodPost, d.HookAddress+"/generateSecurityHeader")
	if err != nil {
		return nil, err
	}
	var result base.Json
	err = json.Unmarshal(res.Body(), &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *AliDrive) refreshDriveId() error {
	infoHeader := map[string]string{
		"host": "user.alipan.com",
	}
	res, err, _ := d.requestS("https://bizapi.alipan.com/v2/user/get", http.MethodPost, nil, infoHeader, nil)
	if err != nil {
		return err
	}
	d.DriveId = utils.Json.Get(res, d.DriveType+"_drive_id").ToString()
	return nil
}

func (d *AliDrive) getAccessToken() string {
	if d.ref != nil {
		return d.ref.getAccessToken()
	}
	return d.AccessToken
}
