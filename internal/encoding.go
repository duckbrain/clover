package internal

import (
	"encoding"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type Value struct {
	V interface{}
}

func processStructTag(tagStr string) (string, bool) {
	tags := strings.Split(tagStr, ",")
	name := tags[0] // when tagStr is "", tags[0] will also be ""
	omitempty := len(tags) > 1 && tags[1] == "omitempty"
	return name, omitempty
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func normalizeStruct(structValue reflect.Value) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	for i := 0; i < structValue.NumField(); i++ {
		fieldType := structValue.Type().Field(i)
		fieldValue := structValue.Field(i)

		if fieldType.PkgPath == "" {
			fieldName := fieldType.Name

			cloverTag := fieldType.Tag.Get("clover")
			name, omitempty := processStructTag(cloverTag)
			if name != "" {
				fieldName = name
			}

			if !omitempty || !isEmptyValue(fieldValue) {
				normalized, err := Normalize(structValue.Field(i).Interface())
				if err != nil {
					return nil, err
				}

				if !fieldType.Anonymous {
					m[fieldName] = normalized
				} else {
					if normalizedMap, ok := normalized.(map[string]interface{}); ok {
						for k, v := range normalizedMap {
							m[k] = v
						}
					} else {
						m[fieldName] = normalized
					}
				}
			}
		}
	}

	return m, nil
}

func normalizeSlice(sliceValue reflect.Value) (interface{}, error) {
	if sliceValue.Type().Elem().Kind() == reflect.Uint8 {
		return sliceValue.Interface(), nil
	}

	s := make([]interface{}, 0)
	for i := 0; i < sliceValue.Len(); i++ {
		v, err := Normalize(sliceValue.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		s = append(s, v)
	}
	return s, nil
}

func getElemValueAndType(v interface{}) (reflect.Value, reflect.Type) {
	rv := reflect.ValueOf(v)
	rt := reflect.TypeOf(v)

	for rt.Kind() == reflect.Ptr && !rv.IsNil() {
		rt = rt.Elem()
		rv = rv.Elem()
	}
	return rv, rt
}

func normalizeMap(mapValue reflect.Value) (map[string]interface{}, error) {
	if mapValue.Type().Key().Kind() != reflect.String {
		return nil, fmt.Errorf("map key type must be a string")
	}

	m := make(map[string]interface{})
	for _, key := range mapValue.MapKeys() {
		value := mapValue.MapIndex(key)

		normalized, err := Normalize(value.Interface())
		if err != nil {
			return nil, err
		}
		m[key.String()] = normalized
	}
	return m, nil
}

func Normalize(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch value := value.(type) {
	case time.Time, Value:
		return value, nil
	case *time.Time:
		return *value, nil
	case encoding.BinaryMarshaler:
		return value.MarshalBinary()
	}

	rValue, rType := getElemValueAndType(value)
	if rType.Kind() == reflect.Ptr {
		return nil, nil
	}

	switch rType.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rValue.Uint(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rValue.Int(), nil
	case reflect.Float32, reflect.Float64:
		return rValue.Float(), nil
	case reflect.Struct:
		return normalizeStruct(rValue)
	case reflect.Map:
		return normalizeMap(rValue)
	case reflect.String:
		return rValue.String(), nil
	case reflect.Bool:
		return rValue.Bool(), nil
	case reflect.Slice, reflect.Array:
		return normalizeSlice(rValue)
	}
	return nil, fmt.Errorf("invalid dtype %s", rType.Name())
}
