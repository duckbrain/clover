package index

import (
	"time"

	"github.com/dgraph-io/badger/v3"
)

type IndexType int

const (
	IndexSingleField IndexType = iota
	IndexGeoSpatial
)

type IndexInfo struct {
	Field string
	Type  IndexType
}

type GeoSpatialIndexInfo struct {
	MinRange float64
	MaxRange float64
}

type Index interface {
	Add(docId string, v interface{}, ttl time.Duration) error
	Remove(docId string, v interface{}) error
	Iterate(reverse bool, onValue func(docId string) error) error
	Drop() error
	Type() IndexType
	Collection() string
	Field() string
}

type indexBase struct {
	collection, field string
}

func (idx *indexBase) Collection() string {
	return idx.collection
}

func (idx *indexBase) Field() string {
	return idx.field
}

type IndexQuery interface {
	Run(onValue func(docId string) error) error
}

func CreateBadgerIndex(collection, field string, idxType IndexType, txn *badger.Txn) Index {
	indexBase := indexBase{collection: collection, field: field}
	switch idxType {
	case IndexSingleField:
		return &badgerRangeIndex{
			indexBase: indexBase,
			txn:       txn,
		}
	}
	return nil
}
