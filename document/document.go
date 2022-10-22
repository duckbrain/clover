package document

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/ostafen/clover/v2/internal"
	"github.com/ostafen/clover/v2/util"
	uuid "github.com/satori/go.uuid"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	ObjectIdField  = "_id"
	ExpiresAtField = "_expiresAt"
)

// Document represents a document as a map.
type Document struct {
	msg    []byte
	fields map[string]interface{}
}

// ObjectId returns the id of the document, provided that the document belongs to some collection. Otherwise, it returns the empty string.
func (doc *Document) ObjectId() string {
	id, _ := doc.Get(ObjectIdField).(string)
	return id
}

// NewDocument creates a new empty document.
func NewDocument() *Document {
	return &Document{}
}

func (doc *Document) mustInitFields() {
	err := doc.initFields()
	if err != nil {
		panic(err)
	}
}

func (doc *Document) initFields() error {
	if doc.fields != nil {
		return nil
	}
	if doc.msg == nil {
		doc.fields = map[string]interface{}{}
		return nil
	}

	fields := map[string]interface{}{}
	err := unmarshal(doc.msg, &fields)
	if err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	normalized, err := internal.Normalize(fields)
	if err != nil {
		return fmt.Errorf("normalize: %w", err)
	}
	fields, _ = normalized.(map[string]interface{})
	if fields == nil {
		panic("should be a map")
	}
	doc.fields = fields
	return nil
}

// NewDocumentOf creates a new document and initializes it with the content of the provided object.
// It returns nil if the object cannot be converted to a valid Document.
func NewDocumentOf(o interface{}) *Document {
	d, _ := NewDocumentFrom(o)
	return d
}

// NewDocumentFrom creates a new document and initializes it with the content of the provided object.
// It returns nil if the object cannot be converted to a valid Document.
func NewDocumentFrom(o interface{}) (*Document, error) {
	msg, err := marshal(o)
	if err != nil {
		return nil, err
	}
	return &Document{msg: msg}, nil
}

// Copy returns a shallow copy of the underlying document.
func (doc *Document) Copy() *Document {
	d := &Document{msg: doc.msg}
	if doc.fields != nil {
		d.fields = util.CopyMap(doc.fields)
	}
	return d
}

func (doc *Document) AsMap() map[string]interface{} {
	doc.mustInitFields()
	return util.CopyMap(doc.fields)
}

func lookupField(name string, fieldMap map[string]interface{}, force bool) (map[string]interface{}, interface{}, string) {
	fields := strings.Split(name, ".")

	var exists bool
	var f interface{}
	currMap := fieldMap
	for i, field := range fields {
		f, exists = currMap[field]

		m, isMap := f.(map[string]interface{})

		if force {
			if (!exists || !isMap) && i < len(fields)-1 {
				m = make(map[string]interface{})
				currMap[field] = m
				f = m
			}
		} else if !exists {
			return nil, nil, ""
		}

		if i < len(fields)-1 {
			currMap = m
		}
	}
	return currMap, f, fields[len(fields)-1]
}

// Has tells returns true if the document contains a field with the supplied name.
func (doc *Document) Has(name string) bool {
	doc.mustInitFields()
	fieldMap, _, _ := lookupField(name, doc.fields, false)
	return fieldMap != nil
}

// Get retrieves the value of a field. Nested fields can be accessed using dot.
func (doc *Document) Get(name string) interface{} {
	doc.mustInitFields()
	_, v, _ := lookupField(name, doc.fields, false)
	return v
}

// Set maps a field to a value. Nested fields can be accessed using dot.
func (doc *Document) Set(name string, value interface{}) {
	doc.mustInitFields()
	normalizedValue, err := internal.Normalize(value)
	if err == nil {
		m, _, fieldName := lookupField(name, doc.fields, true)
		m[fieldName] = normalizedValue
		b, err := marshal(doc.fields)
		if err != nil {
			panic(err)
		}
		doc.msg = b
	}
}

// SetAll sets each field specified in the input map to the corresponding value. Nested fields can be accessed using dot.
func (doc *Document) SetAll(values map[string]interface{}) {
	// TODO More performant encode once
	for updateField, updateValue := range values {
		doc.Set(updateField, updateValue)
	}
}

// GetAll returns a map of all available fields in the document. Nested fields are represented by sub-maps. This is a deep copy, but values are note cloned.
func (doc *Document) ToMap() map[string]interface{} {
	doc.mustInitFields()
	return util.CopyMap(doc.fields)
}

// Fields returns a lexicographically sorted slice of all available field names in the document.
// Nested fields, if included, are represented using dot notation.
func (doc *Document) Fields(includeSubFields bool) []string {
	doc.mustInitFields()
	return util.MapKeys(doc.fields, true, includeSubFields)
}

// ExpiresAt returns the document expiration instant
func (doc *Document) ExpiresAt() *time.Time {
	exp, ok := doc.Get(ExpiresAtField).(time.Time)
	if !ok {
		return nil
	}
	return &exp
}

// ExpiresAt sets document expiration
func (doc *Document) SetExpiresAt(expiration time.Time) {
	doc.Set(ExpiresAtField, expiration)
}

// TTL returns a duration representing the time to live of the document before expiration.
// A negative duration means that the document has no expiration, while a zero value represents an already expired document.
func (doc *Document) TTL() time.Duration {
	expiresAt := doc.ExpiresAt()
	if expiresAt == nil {
		return time.Duration(-1)
	}

	now := time.Now()

	if expiresAt.Before(now) { // document already expired
		return time.Duration(0)
	}

	return time.Millisecond * time.Duration(expiresAt.Sub(now).Milliseconds())
}

// Unmarshal stores the document in the value pointed by v.
func (doc *Document) Unmarshal(v interface{}) error {
	return unmarshal([]byte(doc.msg), v)
}

func isValidObjectId(id string) bool {
	_, err := uuid.FromString(id)
	return err == nil
}

func Validate(doc *Document) error {
	if !isValidObjectId(doc.ObjectId()) {
		return fmt.Errorf("invalid _id: %s", doc.ObjectId())
	}

	if doc.Has(ExpiresAtField) && doc.ExpiresAt() == nil {
		return fmt.Errorf("invalid _expiresAt: %s", doc.Get(ExpiresAtField))
	}
	return nil
}

func Decode(data []byte) (*Document, error) {
	return &Document{msg: data}, nil
}

func Encode(doc *Document) ([]byte, error) {
	return doc.msg, nil
}

func marshal(o interface{}) ([]byte, error) {
	b := bytes.Buffer{}
	enc := msgpack.NewEncoder(&b)
	enc.SetCustomStructTag("clover")
	enc.UseCompactInts(true)
	err := enc.Encode(o)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func unmarshal(b []byte, o interface{}) error {
	dec := msgpack.NewDecoder(bytes.NewReader(b))
	dec.SetCustomStructTag("clover")
	return dec.Decode(o)
}
