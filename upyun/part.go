package upyun

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
)

type chunk struct {
	Id   int
	data []byte
}

type uploadPartContext struct {
	// 多线程上传任务队列
	queue chan *chunk
	// 记录上传时的错误
	errout chan error

	partId int
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type multipartUploader struct {
	fsize    int64
	reader   io.ReadSeeker
	config   *PutObjectConfig
	records  []*DisorderPart
	partSize func(int64) int64
}

func errorJoin(c chan error) error {
	if len(c) == 0 {
		return nil
	}
	s := make([]error, 0)
	for i := range c {
		s = append(s, i)
	}
	return errors.Join(s...)
}

// 创建一个map，用于加速获取已经上传完成的ID
func (p *multipartUploader) skips() map[int64]int64 {
	result := make(map[int64]int64)
	if p.records == nil {
		return result
	}

	for _, v := range p.records {
		result[v.ID] = v.Size
	}
	return result
}

func (p *multipartUploader) product(ctx *uploadPartContext) {
	defer close(ctx.queue)

	skips := p.skips()
	for {
		select {
		case <-ctx.ctx.Done():
			return
		default:
			// 如果分片已经存在，则通过 seek 跳过
			if size, ok := skips[int64(ctx.partId)]; ok {
				if _, err := p.reader.Seek(size, io.SeekCurrent); err != nil {
					fmt.Println(err)
					return
				}
				break
			}

			buffer := make([]byte, p.partSize(p.fsize))
			n, err := p.reader.Read(buffer)
			if err != nil {
				return
			}
			ctx.queue <- &chunk{
				Id:   ctx.partId,
				data: buffer[:n],
			}
		}

		ctx.partId++
	}
}

func (p *multipartUploader) work(ctx *uploadPartContext, fn func(id int, data []byte) error) {
	defer ctx.wg.Done()
	for {
		select {
		case <-ctx.ctx.Done():
			return
		case ch, ok := <-ctx.queue:
			if !ok {
				return
			}
			if err := fn(ch.Id, ch.data); err != nil {
				ctx.errout <- err
				ctx.cancel()
				return
			}
		}
	}
}

func (p *multipartUploader) Go(fn func(id int, data []byte) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uploadCtx := &uploadPartContext{
		ctx:    ctx,
		cancel: cancel,
		errout: make(chan error, p.config.MultipartUploadWorkers),
		queue:  make(chan *chunk, p.config.MultipartUploadWorkers),
	}

	// 如果没有设置分片大小计算器，则使用默认的计算器
	if p.partSize == nil {
		p.partSize = getPartSize
	}

	sort.Slice(p.records, func(i, j int) bool {
		return p.records[i].ID > p.records[j].ID
	})

	// 生成任务
	go p.product(uploadCtx)

	// 消费任务
	for i := 0; i < p.config.MultipartUploadWorkers; i++ {
		uploadCtx.wg.Add(1)
		go p.work(uploadCtx, fn)
	}

	uploadCtx.wg.Wait()
	close(uploadCtx.errout)

	return errorJoin(uploadCtx.errout)
}
