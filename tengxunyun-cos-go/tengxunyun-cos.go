package tengxunyun_cos_go

import (
	"context"
	"fmt"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"net/http"
	"net/url"
)

type Client struct {
	c *cos.Client
}

func NewClient(bucketName string, secretID, secretKey string) (Client, error) {

	bucketUrl := fmt.Sprintf("https://%v-1306582638.cos.ap-shanghai.myqcloud.com", bucketName)
	u, err := url.Parse(bucketUrl)
	if err != nil {
		return Client{}, err
	}
	b := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  secretID,
			SecretKey: secretKey,
		},
	})

	return Client{c: client}, err
}

func NewBucket(bucketName string, opt *cos.BucketPutOptions, secretID, secretKey string) *cos.Response {
	bucketUrl := fmt.Sprintf("https://%v-1306582638.cos.ap-shanghai.myqcloud.com", bucketName)
	u, _ := url.Parse(bucketUrl)

	b := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  secretID,
			SecretKey: secretKey,
		},
	})

	response, err := client.Bucket.Put(context.Background(), opt)
	if response != nil {
		if response.StatusCode == 409 {
			err = nil
		}
	}
	if err != nil {
		panic(err)
	}

	return response
}

func (client Client) GetBucket() *cos.ServiceGetResult {

	result, _, err := client.c.Service.Get(context.Background())
	if err != nil {
		panic(err)
	}
	for _, v := range result.Buckets {
		fmt.Println(v.Name)
		fmt.Println(v.CreationDate)
		fmt.Println(v.Region)
		fmt.Println()
	}
	return result
}

//列出对象列表
func (client Client) GetObject(prefix string, maxKeys int) (*cos.BucketGetResult, *cos.Response) {
	opt := &cos.BucketGetOptions{
		Prefix:  prefix,
		MaxKeys: maxKeys,
	}
	results, response, err := client.c.Bucket.Get(context.Background(), opt)
	if err != nil {
		panic(err)
	}

	return results, response
}

//cloudFile 云上路径+文件名
func (client Client) UploadFile(reader io.Reader, contentType string, cloudFile string) *cos.Response {

	size, err := cos.GetReaderLen(reader)
	if err != nil {
		panic(err)
	}

	opt := &cos.ObjectPutOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			ContentType:   contentType,
			ContentLength: size,
		},
	}

	response, err := client.c.Object.Put(context.Background(), cloudFile, reader, opt)
	if err != nil {
		panic(err)
	}
	return response
}

//localFile 下载到的本地路径+文件名
//cloudFile 云上的文件路径+文件名
//
//两种下载方式1.存于response中 2.直接下载到本地路径
func (client Client) DownloadFile(cloudFile string, localFile string, downType int) *cos.Response {

	if downType == 1 {
		response, err := client.c.Object.Get(context.Background(), cloudFile, nil)
		if err != nil {
			panic(err)
		}
		return response
	} else { //直接下载到本地路径
		response, err := client.c.Object.GetToFile(context.Background(), cloudFile, localFile, nil)
		if err != nil {
			panic(err)
		}
		return response
	}
}

//cloudFile 云上的文件路径+文件名
func (client Client) Delete(cloudFile string) *cos.Response {
	response, err := client.c.Object.Delete(context.Background(), cloudFile)
	if err != nil {
		panic(err)
	}
	return response
}

//批量删除
//cloudFiles 云上文件路径+文件名
func (client Client) MultiDelete(cloudFiles []string) (*cos.ObjectDeleteMultiResult, *cos.Response) {

	obs := make([]cos.Object, 0, len(cloudFiles))
	for _, v := range cloudFiles {
		obs = append(obs, cos.Object{Key: v})
	}

	opt := &cos.ObjectDeleteMultiOptions{
		Objects: obs,
		Quiet:   true,
	}

	result, response, err := client.c.Object.DeleteMulti(context.Background(), opt)
	if err != nil {
		panic(err)
	}

	return result, response

}

//获取对象元数据(Meta)
func (client Client) ObjectHead(cloudFiles string) *cos.Response {

	response, err := client.c.Object.Head(context.Background(), cloudFiles, nil)
	if err != nil {
		panic(err)
	}
	return response
}

//初始化分块上传
//
//cloudFile 文件key  即云路径+文件名
//返回的result中有uploadID 可用于中止上传
func (client Client) InitMultipartUpload(opt *cos.InitiateMultipartUploadOptions, cloudFile string) (*cos.InitiateMultipartUploadResult, *cos.Response) {

	opt = &cos.InitiateMultipartUploadOptions{
		ACLHeaderOptions:       nil,
		ObjectPutHeaderOptions: nil,
	}

	result, response, err := client.c.Object.InitiateMultipartUpload(context.Background(), cloudFile, opt)
	if err != nil {
		panic(err)
	}

	return result, response
}

//中止分块上传
//
//cloudFile 文件key,
//
//uploadID在初始化时返回的result中
func (client Client) AbortMultipartUpload(cloudFile string, uploadID string) *cos.Response {

	response, err := client.c.Object.AbortMultipartUpload(context.Background(), cloudFile, uploadID)
	if err != nil {
		panic(err)
	}

	return response
}

//上传分块
//
//opt {ContentLength : 设置传输长度} 如果传入的reader不是bytes.Buffer/bytes.Reader/strings.Reader，则必须设置opt
//
//partNumber 标识上传分块的序号
func (client Client) UploadPart(cloudFile, uploadID string, reader io.Reader, partNumber int, opt *cos.ObjectUploadPartOptions) *cos.Response {

	response, err := client.c.Object.UploadPart(context.Background(), cloudFile, uploadID, partNumber, reader, opt)
	if err != nil {
		panic(err)
	}

	return response
}

//列出指定uploadID中已经上传的分块信息
func (client Client) ListParts(cloudFile, uploadID string, opt *cos.ObjectListPartsOptions) (*cos.ObjectListPartsResult, *cos.Response) {

	result, response, err := client.c.Object.ListParts(context.Background(), cloudFile, uploadID, opt)
	if err != nil {
		panic(err)
	}

	return result, response
}

//完成分块上传
func (client Client) CompleteMultipartUpload(cloudFile, uploadID string, opt *cos.CompleteMultipartUploadOptions) (*cos.CompleteMultipartUploadResult, *cos.Response) {

	result, response, err := client.c.Object.CompleteMultipartUpload(context.Background(), cloudFile, uploadID, opt)
	if err != nil {
		panic(err)
	}

	return result, response
}

/*
opt :

1. Prefix 默认为空，对 object 的 key 进行筛选，匹配 prefix 为前缀的 objects

2. Delimiter 默认为空，设置分隔符，比如设置/来模拟文件夹

3.EncodingType 默认不编码，规定返回值的编码方式，可选值：url

4.Marker 默认以 UTF-8 二进制顺序列出条目，标记返回 objects 的 list 的起点位置

5.MaxKeys 最多返回的 objects 数量，默认为最大的1000
*/
func (client Client) ListObject(opt *cos.BucketGetOptions) (*cos.BucketGetResult, *cos.Response) {

	result, response, err := client.c.Bucket.Get(context.Background(), opt)
	if err != nil {
		panic(err)
	}

	return result, response
}

func IsDir(object cos.Object) bool {
	return object.Key[len(object.Key)-1:] == "/"
}
