package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawGetResponse{NotFound: true}
	r, err := server.storage.Reader(nil)
	defer r.Close()
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	value, err := r.GetCF(req.Cf, req.Key)
	if err != nil {
		return resp, nil
	}
	resp.Value = value
	resp.NotFound = value == nil
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := &kvrpcpb.RawPutResponse{}
	modify := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	}
	if err := server.storage.Write(nil, modify); err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := &kvrpcpb.RawDeleteResponse{}
	modify := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	}
	if err := server.storage.Write(nil, modify); err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := &kvrpcpb.RawScanResponse{}
	r, err := server.storage.Reader(nil)
	defer r.Close()
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	it := r.IterCF(req.Cf)
	limit := req.Limit
	var cnt uint32 = 0
	for it.Seek(req.StartKey); it.Valid(); it.Next() {
		item := it.Item()
		kv := &kvrpcpb.KvPair{Key: item.KeyCopy(nil)}
		if val, err := item.ValueCopy(nil); err != nil {
			kv.Value = nil
		} else {
			kv.Value = val
		}
		resp.Kvs = append(resp.Kvs, kv)
		cnt++
		if cnt >= limit {
			break
		}
	}
	return resp, nil
}
