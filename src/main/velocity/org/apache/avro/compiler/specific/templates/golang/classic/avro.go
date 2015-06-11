/**
 * Copyright (c) 2014 Raytheon BBN Technologies Corp
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ${outputPackageName}
#[[

/**
	Golang wrappers around the avro C API

	The reason all of the types are in the same file is because cgo seems to
	randomly get confused if you try to share C types across packages/etc.
*/


/*
#cgo LDFLAGS: -lavro
#include <stdlib.h>
#include <avro.h>


avro_file_writer_t my_avro_file_writer(FILE * fp, avro_schema_t schema) {
	avro_file_writer_t fw;
	if (!avro_file_writer_create_fp(fp, "nullpath", 1, schema, &fw))
		return fw;
	return NULL; 
}

avro_file_reader_t my_avro_file_reader(FILE * fp)
{
	avro_file_reader_t fr;
	if (!avro_file_reader_fp(fp, "nullpath", 1, &fr))
		return fr;
	return NULL;
}

// helpers to pass values in

avro_datum_t avro_my_givestring(const char * st)
{
	return avro_givestring(st, (void*)free);
}

void nullfree(void* p){}

avro_datum_t avro_my_bytes(void * bytes, int64_t len)
{
	// use givebytes to avoid the copy, but don't actually free the buffer,
	// because golang owns it!
	return avro_givebytes(bytes, len, (void*)nullfree); 
}

avro_datum_t avro_my_fixed(avro_schema_t schema, void * bytes, int64_t len)
{
	// use givefixed to avoid the copy, but don't actually free the buffer,
	// because golang owns it!
	return avro_givefixed(schema, bytes, len, (void*)nullfree); 
}

int avro_datum_typeof(avro_datum_t datum) 
{
	return avro_typeof(datum);
}

int avro_schema_typeof(avro_schema_t schema) 
{
	return avro_typeof(schema);
}

// BEGIN NASTY HACK


struct avro_record_schema_hack_t {
	struct avro_obj_t obj;
	char *name;
	char *space;
};

#define container_of(ptr_, type_, member_)  \
    ((type_ *)((char *)ptr_ - (size_t)&((type_ *)0)->member_))
#define avro_schema_to_record_hack(schema_)  (container_of(schema_, struct avro_record_schema_hack_t, obj))

const char *avro_schema_namespace(const avro_schema_t schema)
{
	if (is_avro_record(schema)) {
		return (avro_schema_to_record_hack(schema))->space;
	}
	
	return NULL;
}

// END NASTY HACK


// returns the schema associated with the array part of the schema (there is only one)
avro_schema_t avro_schema_find_union_array_schema(avro_schema_t union_schema)
{
	int i;
	size_t count = avro_schema_union_size(union_schema);
	if (count == EINVAL)
		return NULL;
		
	for (i = 0; i < count; i++)
	{
		avro_schema_t schema = avro_schema_union_branch(union_schema, i);
		if (avro_typeof(schema) == AVRO_ARRAY)
			return schema;
	}
	
	return NULL;
}

// returns the schema associated with the map part of the schema (there is only one)
avro_schema_t avro_schema_find_union_map_schema(avro_schema_t union_schema)
{
	int i;
	size_t count = avro_schema_union_size(union_schema);
	if (count == EINVAL)
		return NULL;
		
	for (i = 0; i < count; i++)
	{
		avro_schema_t schema = avro_schema_union_branch(union_schema, i);
		if (avro_typeof(schema) == AVRO_MAP)
			return schema;
	}
	
	return NULL;
}

// returns the matching discriminant
int64_t avro_schema_find_union_discriminant(avro_schema_t union_schema, avro_schema_t value_schema)
{
	int i;
	size_t count = avro_schema_union_size(union_schema);
	if (count == EINVAL)
	{
		//fprintf(stderr, "OH NO");
		return -1;
	}

	//fprintf(stderr, "%p %p\n", union_schema, value_schema);

	for (i = 0; i < count; i++)
	{
		avro_schema_t branch_schema = avro_schema_union_branch(union_schema, i);
		if (is_avro_link(branch_schema))
			branch_schema = avro_schema_link_target(branch_schema);
		
		if (avro_schema_equal(branch_schema, value_schema))
			return i;
	}
	
	return -1;
}



*/
import "C"
import (
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"strconv"
	"unsafe"
)


// define basic avro interface type
type AvroObject interface {
	
	GetSchema() *AvroSchema					// used in generic way
	ToRecord() (*AvroRecord, error)
	ToJSON(bool) (string, error)
}

//
// Generic avro operations
//

// keyed by namespace
var RecordDecoders = make(map[string]func(*AvroRecord) (AvroObject, error))

func AvroRegisterSchema(decoder func(*AvroRecord) (AvroObject, error), json string) *AvroSchema {
	schema := AvroParseSchema(json)
	RecordDecoders[schema.GetName()] = decoder
	return schema
}

// AvroFromRecord only works on registered schema types, so it won't work on
// primitive avro types (though in principle there's no reason that wouldn't
// work)
func AvroFromRecord(record *AvroRecord) (AvroObject, error) {
	schema_name := record.GetSchemaName()
	decoder, ok := RecordDecoders[schema_name]
	if !ok {
		return nil, errors.New("A decoder does not exist for " + schema_name) 
	}
	return decoder(record)
}

// Avro primitive schemas
var (
	StringSchema 	= wrapAvroSchema(C.avro_schema_string())
	BytesSchema 	= wrapAvroSchema(C.avro_schema_bytes())
	IntSchema 		= wrapAvroSchema(C.avro_schema_int())
	LongSchema 		= wrapAvroSchema(C.avro_schema_long())
	FloatSchema 	= wrapAvroSchema(C.avro_schema_float())
	DoubleSchema 	= wrapAvroSchema(C.avro_schema_double())
	BooleanSchema 	= wrapAvroSchema(C.avro_schema_boolean())
	NullSchema 		= wrapAvroSchema(C.avro_schema_null())
)

//
// Avro schema wrapper
//

type AvroSchema struct {
	schema C.avro_schema_t
	full_name string
}

// AvroParseSchema parses a json string and creates a schema object from it
func AvroParseSchema(json string) *AvroSchema {
	
	cjson := C.CString(json)
	defer C.free(unsafe.Pointer(cjson))
	
	var schema C.avro_schema_t
	
	if C.avro_schema_from_json_length(cjson, C.size_t(len(json)), &schema) != 0 {
		log.Panic("Invalid schema: " + C.GoString(C.avro_strerror()) + ": " + json)
	}
	
	return wrapAvroSchema(schema)
}

func wrapAvroSchema(schema C.avro_schema_t) *AvroSchema {
	wrapper := &AvroSchema{schema, ""}
	runtime.SetFinalizer(wrapper, finalizeSchema)
	return wrapper
}

func (this *AvroSchema) CreateRecord() *AvroRecord {
	return wrapAvroRecord(C.avro_record(this.schema))
}

func (this *AvroSchema) GetName() string {
	if this.full_name == "" {
		ns := C.avro_schema_namespace(this.schema)
		if ns == nil {
			this.full_name = C.GoString(C.avro_schema_name(this.schema)) 
		} else {
			this.full_name = C.GoString(ns) + "." + C.GoString(C.avro_schema_name(this.schema))
		}
	}
	return this.full_name
}

// this is slower, but safer than using avro_schema_record_field_get()
func (this *AvroSchema) GetFieldSchema(field string) *AvroSchema {
	cfield := C.CString(field)
	defer C.free(unsafe.Pointer(cfield))
	index := C.avro_schema_record_field_get_index(this.schema, cfield)
	if index == -1 {
		return nil
	}
	return &AvroSchema{C.avro_schema_record_field_get_by_index(this.schema, index), ""}
}

func (this *AvroSchema) GetFieldSchemaUnsafe(field string) *AvroSchema {
	cfield := C.CString(field)
	defer C.free(unsafe.Pointer(cfield))
	return &AvroSchema{C.avro_schema_record_field_get(this.schema, cfield), ""}
}

// schema for the items in the array
func (this *AvroSchema) GetArraySchema() *AvroSchema {
	// no reference counting
	return &AvroSchema{C.avro_schema_array_items(this.schema), ""}
}

// schema for the items in the map
func (this *AvroSchema) GetMapSchema() *AvroSchema {
	// no reference counting
	return &AvroSchema{C.avro_schema_map_values(this.schema), ""}
}

// returns the map schema for the union, plus the schema for the items
func (this *AvroSchema) FindUnionArraySchema() (*AvroSchema, *AvroSchema) {
	// no reference counting
	schema := C.avro_schema_find_union_array_schema(this.schema)
	if schema == nil {
		return nil, nil
	}
	wrapper := &AvroSchema{schema, ""}
	return wrapper, wrapper.GetArraySchema() 
}

// returns the array schema for the union, plus the schema for the values
func (this *AvroSchema) FindUnionMapSchema() (*AvroSchema, *AvroSchema) {
	// no reference counting
	schema := C.avro_schema_find_union_map_schema(this.schema)
	if schema == nil {
		return nil, nil
	}
	wrapper := &AvroSchema{schema, ""}
	return wrapper, wrapper.GetMapSchema()
}



//
// Avro record wrapper
//

type AvroRecord struct {
	record C.avro_datum_t
}

// this function is only required when creating a record. Getting a record
// doesn't increment the reference count
func wrapAvroRecord(record C.avro_datum_t) *AvroRecord {
	wrapper := &AvroRecord{record}
	runtime.SetFinalizer(wrapper, finalizeRecord)
	return wrapper
}

// this doesn't return an error because only the only error is if a field
// doesn't exist, and autogenerated code should match the schema. non auto
// generated code shouldn't be using this
func (this *AvroRecord) Get(fieldName string) *AvroDatum {
	
	cFieldName := C.CString(fieldName)
	defer C.free(unsafe.Pointer(cFieldName))
	
	var datum C.avro_datum_t
	
	if C.avro_record_get(this.record, cFieldName, &datum) != 0 {
		return nil
	}
	
	// no reference counting required
	return &AvroDatum{datum}
}

func (this * AvroRecord) Set(fieldName string, datum *AvroDatum) error {
	cfieldName := C.CString(fieldName)
	defer C.free(unsafe.Pointer(cfieldName))
	
	//str, err := this.ToJSON(false)
	
	//log.Println("Setting: " + str + ": " + err.Error())
	
	
    if (C.avro_record_set(this.record, cfieldName, datum.datum) != 0) {
        return errors.New("failed to set '" + fieldName + "': " + C.GoString(C.avro_strerror()))
    }
    
    var tmp C.avro_datum_t
    if C.avro_record_get(this.record, cfieldName, &tmp) != 0 {
    	return errors.New("Weird... " + C.GoString(C.avro_strerror()))
    }
    
    return nil
}  

func (this *AvroRecord) ToJSON(prettyPrint bool) (string, error) {
	var pp C.int
	if prettyPrint {
		pp = 0
	} else {
		pp = 1
	}
	var json *C.char
	if C.avro_datum_to_json(this.record, pp, &json) != 0 {
		return "", errors.New(C.GoString(C.avro_strerror()))
	}
	defer C.free(unsafe.Pointer(json))
	return C.GoString(json), nil
}

func (this *AvroRecord) GetSchemaName() string {
	schema := &AvroSchema{C.avro_datum_get_schema(this.record), ""}
	return schema.GetName()
}

func (this *AvroRecord) Validate() error {
	schema := C.avro_datum_get_schema(this.record)
	if C.avro_schema_datum_validate(schema, this.record) == 0 {
		return errors.New("Did not validate: " + C.GoString(C.avro_strerror()))
	}
	return nil
}

// fixed types are a special type of record. You get the field that
// they reside in, and call ToRecord() on it. Pass that to the fixed
// from record function, and it will call GetFixed directly

func (this *AvroRecord) GetFixed() ([]byte, error) {
	return avroGetFixed(this.record)
}

func RecordFromFixed(schema *AvroSchema, bytes []byte) (*AvroRecord, error) {
	datum := C.avro_my_fixed(schema.schema, unsafe.Pointer(&bytes[0]), C.int64_t(len(bytes)))
	if datum == nil {
		return nil, errors.New("failed to create fixed element: " + C.GoString(C.avro_strerror()))
	}
	
	return wrapAvroRecord(datum), nil
}


//
// Avro datum wrapper
//

type AvroDatum struct {
	datum C.avro_datum_t
}

// this function is only required when creating a datum. Getting a datum
// doesn't increment the reference count.
func wrapAvroDatum(datum C.avro_datum_t) (*AvroDatum, error) {
	if datum == nil {
		return nil, errors.New("failed to create datum: " + C.GoString(C.avro_strerror()))
	}
	
	wrapper := &AvroDatum{datum}
	runtime.SetFinalizer(wrapper, finalizeDatum)
	return wrapper, nil
}


func DatumFromString(data string) (*AvroDatum, error) {
	return wrapAvroDatum(C.avro_my_givestring(C.CString(data)))
}

//assumes byte contents dont change!
func DatumFromBytes(bytes []byte) (*AvroDatum, error) {
	return wrapAvroDatum(C.avro_my_bytes(unsafe.Pointer(&bytes[0]), C.int64_t(len(bytes))))
}

func DatumFromInt(data int32) (*AvroDatum, error){
	return wrapAvroDatum(C.avro_int32(C.int32_t(data)))
}

func DatumFromLong(data int64) (*AvroDatum, error){
	return wrapAvroDatum(C.avro_int64(C.int64_t(data)))
}

func DatumFromFloat(data float32) (*AvroDatum, error) {
	return wrapAvroDatum(C.avro_float(C.float(data)))
}

func DatumFromDouble(data float64) (*AvroDatum, error) {
	return wrapAvroDatum(C.avro_double(C.double(data)))
}

func DatumFromBoolean(data bool)  (*AvroDatum, error) {
	if data {
		return wrapAvroDatum(C.avro_boolean(1))	
	} else {
		return wrapAvroDatum(C.avro_boolean(0))
	}
}

func DatumFromNull()  (*AvroDatum, error) {
	return wrapAvroDatum(C.avro_null())
}

func DatumFromEnum(schema *AvroSchema, data int) (*AvroDatum, error) {
	return wrapAvroDatum(C.avro_enum(schema.schema, C.int(data)))
}

// only intended for enum
func DatumFromRecord(record *AvroRecord) (*AvroDatum, error) {
	C.avro_datum_incref(record.record)
	return wrapAvroDatum(record.record)
}

// only intended for enum
func RecordFromDatum(datum *AvroDatum) (*AvroRecord, error) {
	return &AvroRecord{datum.datum}, nil
}

// creates a datum from any schema + value
func DatumFromAny(schema *AvroSchema, data interface{}) (*AvroDatum, error) {
	
	switch C.avro_schema_typeof(schema.schema) {
		case C.AVRO_STRING:
			return DatumFromString(data.(string))
		case C.AVRO_BYTES:
			return DatumFromBytes(data.([]byte))
		case C.AVRO_INT32:
			return DatumFromInt(data.(int32))
		case C.AVRO_INT64:
			return DatumFromLong(data.(int64))
		case C.AVRO_FLOAT:
			return DatumFromFloat(data.(float32))
		case C.AVRO_DOUBLE:
			return DatumFromDouble(data.(float64))
		case C.AVRO_BOOLEAN:
			return DatumFromBoolean(data.(bool))
		case C.AVRO_NULL:
			return DatumFromNull()
		case C.AVRO_RECORD, C.AVRO_ENUM, C.AVRO_FIXED:
			rec, err := data.(AvroObject).ToRecord()
			if err != nil {
				return nil, err
			}
			return DatumFromRecord(rec)
		case C.AVRO_MAP:
			dmap, err := DatumMap(schema)
			values_schema := schema.GetMapSchema()
			if err == nil {
				for k, v := range data.(map[string]interface{}) {
					dv, err := DatumFromAny(values_schema, v)
					if err != nil {
						return nil, err
					}
					if err = dmap.Set(k, dv); err != nil {
						return nil, err
					}
				}
			} 
			return dmap.ToDatum()
		case C.AVRO_ARRAY:
			array := data.([]interface{})
			darray, err := DatumArray(schema)
			values_schema := schema.GetArraySchema()
			if err == nil {
				for i := 0; i < len(array); i++ {
					d, err := DatumFromAny(values_schema, array[i])
					if err != nil {
						return nil, err
					}
					if err = darray.Append(d); err != nil {
						return nil, err
					}	
				}
			}
			return darray.ToDatum()	
		case C.AVRO_UNION:
			return DatumFromUnion(schema, data)
	}
	
	return nil, errors.New("Unsupported schema type")
}

func DatumFromUnion(union_schema *AvroSchema, data interface{}) (*AvroDatum, error) {
	
	var datum *AvroDatum
	var err error
	
	// first, create the datum
	// then, figure out which branch the union belongs to
	
	if data == nil {
		datum, err = DatumFromNull()
	} else {
	
		switch t := data.(type) {
		case string:
			datum, err = DatumFromString(t)
		case []byte:
			datum, err = DatumFromBytes(t)
		case int32:
			datum, err = DatumFromInt(t)
		case int64:
			datum, err = DatumFromLong(t)
		case float32:
			datum, err = DatumFromFloat(t)
		case float64:
			datum, err = DatumFromDouble(t)
		case bool:
			datum, err = DatumFromBoolean(t)	
		case AvroObject:
			rec, err := t.ToRecord()
			if err != nil {
				return nil, err
			}
			datum, err = DatumFromRecord(rec)
		case map[string]interface{}:
			// need a schema to create a map
			map_schema, values_schema := union_schema.FindUnionMapSchema()
			if map_schema == nil {
				return nil, errors.New("No map found in union schema")	
			}
			dmap, err := DatumMap(map_schema)
			if err == nil {
				for k, v := range t {
					dv, err := DatumFromAny(values_schema, v)
					if err != nil {
						return nil, err
					}
					if err = dmap.Set(k, dv); err != nil {
						return nil, err
					}
				}
			} 
			datum, err = dmap.ToDatum()
		case []interface{}:
			array_schema, values_schema := union_schema.FindUnionArraySchema()
			if array_schema == nil {
				return nil, errors.New("No array found in union schema")	
			}
			darray, err := DatumArray(array_schema)
			if err == nil {
				for i := 0; i < len(t); i++ {
					d, err := DatumFromAny(values_schema, t[i])
					if err != nil {
						return nil, err
					}
					if err = darray.Append(d); err != nil {
						return nil, err
					}	
				}
			}
			datum, err = darray.ToDatum()	
		default:
			return nil, fmt.Errorf("Unsupported type in union: %#v", t)	
		}
	}
	
	if err != nil {
		return nil, err
	}
	
	// ok, figure out which branch this belongs to
	datum_schema := C.avro_datum_get_schema(datum.datum)
	discriminant := C.avro_schema_find_union_discriminant(union_schema.schema, datum_schema)
	if discriminant == -1 {
		return nil, errors.New("Invalid type " + (&AvroSchema{datum_schema, ""}).GetName() + " passed for union")
	}
	
	// and create the union from it
	return wrapAvroDatum(C.avro_union(union_schema.schema, discriminant, datum.datum))
}


//
// get various types from a datum
//

func (this *AvroDatum) GetAny() (interface{}, error) {
	avro_type := C.avro_datum_typeof(this.datum)
	switch avro_type {
		case C.AVRO_STRING:
			return this.GetString()
		case C.AVRO_BYTES:
			return this.GetBytes()
		case C.AVRO_INT32:
			return this.GetInt()
		case C.AVRO_INT64:
			return this.GetLong()
		case C.AVRO_FLOAT:
			return this.GetFloat()
		case C.AVRO_DOUBLE:
			return this.GetDouble()
		case C.AVRO_BOOLEAN:
			return this.GetBoolean()
		case C.AVRO_NULL:
			return nil, nil
		case C.AVRO_RECORD, C.AVRO_FIXED, C.AVRO_ENUM:
			return AvroFromRecord(this.ToRecord())
		case C.AVRO_MAP:
			gen_map := make(map[string]interface{})
			for i := 0; i < this.GetMapSize(); i++ {
				k, v, err := this.GetMapElement(i) 
				if err != nil {
					return nil, err
				}
				gen_map[k], err = v.GetAny()
				if err != nil {
					return nil, err
				}
			}
			return gen_map, nil
		case C.AVRO_ARRAY:
			array_size := this.GetArraySize()
			gen_array := make([]interface{}, array_size)
			for i := 0; i < array_size; i++ {
				v, err := this.GetArrayElement(i)
				if err != nil {
					return nil, err
				}
				gen_array[i], err = v.GetAny() 
				if err != nil {
					return nil, err
				}
			}
			return gen_array, nil
		case C.AVRO_UNION:
			return this.GetUnion()	
		case -1:
			return nil, nil
	}
	
	return nil, errors.New("Unhandled avro type " + strconv.Itoa(int(avro_type)))
}


func (this *AvroDatum) GetString() (string, error) {
	var data *C.char 
	
	if C.avro_string_get(this.datum, &data) != 0 {
		return "", errors.New(C.GoString(C.avro_strerror()))
	}
	
	return C.GoString(data), nil
}

func (this *AvroDatum) GetBytes() ([]byte, error) {	
	
	var clen C.int64_t 
	var cbytes *C.char
	
	if C.avro_bytes_get(this.datum, &cbytes, &clen) != 0 {
		return nil, errors.New(C.GoString(C.avro_strerror()))
	}
	
	bytes := make([]byte, clen)
	
	// copy the bytes directly into the slice
	C.memcpy(unsafe.Pointer(&bytes[0]), unsafe.Pointer(cbytes), C.size_t(clen))
	
	return bytes, nil
}

func (this *AvroDatum) GetInt() (int32, error) {	
	var data C.int32_t
	
	if C.avro_int32_get(this.datum, &data) != 0 {
		return 0, errors.New(C.GoString(C.avro_strerror()))
	}
	
	return int32(data), nil
}

func (this *AvroDatum) GetLong() (int64, error) {
	var data C.int64_t
	
	if C.avro_int64_get(this.datum, &data) != 0 {
		return 0, errors.New(C.GoString(C.avro_strerror()))
	}
	
	return int64(data), nil
}

func (this *AvroDatum) GetFloat() (float32, error) {
	var data C.float
	
	if C.avro_float_get(this.datum, &data) != 0 {
		return 0, errors.New(C.GoString(C.avro_strerror()))
	}
	
	return float32(data), nil
}

func (this *AvroDatum) GetDouble() (float64, error) {
	var data C.double
	
	if C.avro_double_get(this.datum, &data) != 0 {
		return 0, errors.New(C.GoString(C.avro_strerror()))
	}
	
	return float64(data), nil
}

func (this *AvroDatum) GetEnum() (int, error) {
	return int(C.avro_enum_get(this.datum)), nil
}

func (this *AvroDatum) GetBoolean() (bool, error) {
	var data C.int8_t
	
	if C.avro_boolean_get(this.datum, &data) != 0 {
		return false, errors.New(C.GoString(C.avro_strerror()))
	}
	
	if data == 0 {
		return false, nil
	} else {
		return true, nil
	}
}

func (this *AvroDatum) GetNull() (interface{}, error) {
	return nil, nil
}
 
// warning: the returned record is only valid as long as its parent object,
// cannot reference count it since that isn't always valid (like in an array)
func (this *AvroDatum) ToRecord() *AvroRecord {
	return &AvroRecord{this.datum}
}

func (this *AvroDatum) GetUnion() (interface{}, error) {
	// doesn't increment the reference count, no wrapper required
	current_branch := C.avro_union_current_branch(this.datum)
	if current_branch == nil {
		if C.avro_union_discriminant(this.datum) == -1 {
			return nil, nil
		}
		return nil, errors.New("No current branch for union!")
	}
	
	return (&AvroDatum{current_branch}).GetAny()
}

func (this *AvroDatum) GetMapSize() int {
	return int(C.avro_map_size(this.datum))
}

// returns key, value
// warning: datum is only valid while parent is alive
func (this *AvroDatum) GetMapElement(i int) (string, *AvroDatum, error) {
	var ckey *C.char
	var datum C.avro_datum_t
	
	if C.avro_map_get_key(this.datum, C.int(i), &ckey) != 0 {
		return "", nil, errors.New(C.GoString(C.avro_strerror()))
	}
	
	if C.avro_map_get(this.datum, ckey, &datum) != 0 {
		return "", nil, errors.New(C.GoString(C.avro_strerror()))
	}
	
	return C.GoString(ckey), &AvroDatum{datum}, nil
}

func (this *AvroDatum) GetArraySize() int {
	return int(C.avro_array_size(this.datum))
}

// warning: datum is only valid while parent is alive
func (this *AvroDatum) GetArrayElement(i int) (*AvroDatum, error) {
	var datum C.avro_datum_t
	if C.avro_array_get(this.datum, C.int64_t(i), &datum) != 0 {
		return nil, errors.New(C.GoString(C.avro_strerror()))
	}
	
	return &AvroDatum{datum}, nil
}

//
// Array/Map wrappers
//

type AvroArray struct {
	datum C.avro_datum_t
}

func DatumArray(schema *AvroSchema) (*AvroArray, error) {
	datum := C.avro_datum_from_schema(schema.schema)
	if datum == nil {
		return nil, errors.New("failed to create array: " + C.GoString(C.avro_strerror()))
	}
	
	wrapper := &AvroArray{datum}
	runtime.SetFinalizer(wrapper, finalizeArray)
	return wrapper, nil
}

func (this *AvroArray) Append(datum *AvroDatum) error {
	if C.avro_array_append_datum(this.datum, datum.datum) != 0 {
		return errors.New("append failed: " + C.GoString(C.avro_strerror()))
	}
	
	return nil
}

func (this *AvroArray) ToDatum() (*AvroDatum, error) {
	C.avro_datum_incref(this.datum)
	return wrapAvroDatum(this.datum)
}


type AvroMap struct {
	datum C.avro_datum_t
}

func DatumMap(schema *AvroSchema) (*AvroMap, error) {
	datum := C.avro_datum_from_schema(schema.schema)
	if datum == nil {
		return nil, errors.New("failed to create array: " + C.GoString(C.avro_strerror()))
	}
	
	wrapper := &AvroMap{datum}
	runtime.SetFinalizer(wrapper, finalizeMap)
	return wrapper, nil
}

func (this *AvroMap) Set(key string, datum *AvroDatum) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))
	
	if C.avro_map_set(this.datum, ckey, datum.datum) != 0 {
		return errors.New("failed to set '" + key + "': " + C.GoString(C.avro_strerror()))
	}
	
	return nil
}

func (this *AvroMap) ToDatum() (*AvroDatum, error) {
	C.avro_datum_incref(this.datum)
	return wrapAvroDatum(this.datum)
}


//
// File reader wrapper
//

type AvroContainerReader struct {
	reader C.avro_file_reader_t
	fp *C.FILE
}

// NewAvroContainerBytesReader opens an avro container reader that reads from
// a memory buffer. Don't forget to close it!
func NewAvroContainerBytesReader(bytes []byte) (reader *AvroContainerReader, err error) {
	if len(bytes) == 0 {
		return nil, errors.New("Cannot deserialize a zero length array")
	}
	
	fp := fmemopen(bytes)
	if fp == nil {
		return nil, errors.New("Could not create memory stream")
	}
	
	// don't need to close fp, it will do it for us, even on error
	return NewAvroContainerFileReader(fp)
}

// NewAvroContainerFileReader opens an avro container reader that reads from
// a file descriptor. Don't forget to close it!
func NewAvroContainerFileReader(fp *C.FILE) (*AvroContainerReader, error) {
	reader := C.my_avro_file_reader(fp)
	if reader == nil {
		return nil, errors.New("Creating reader: " + C.GoString(C.avro_strerror()))
	}
	return &AvroContainerReader{reader, fp}, nil
}

func (this *AvroContainerReader) GetWriterSchema() *AvroSchema {
	return &AvroSchema{C.avro_file_reader_get_writer_schema(this.reader), ""}
}

// Read reads a single avro record from the readed, but does not deserialize it to golang
func (this *AvroContainerReader) Read(schema *AvroSchema) (*AvroRecord, error) {
	var record C.avro_datum_t
	if C.avro_file_reader_read(this.reader, schema.schema, &record) != 0 {
		if C.feof(this.fp) != 0 {
			return nil, io.EOF
		}
		return nil, errors.New("Error reading record: " + C.GoString(C.avro_strerror()))
	}
	
	return wrapAvroRecord(record), nil
}

// ReadRecords reads all the records out of an avro container into a slice
func (this *AvroContainerReader) ReadRecords(schema *AvroSchema) (objects []AvroObject, err error) {
	
	schema_name := schema.GetName()
	fromRecord, ok := RecordDecoders[schema_name]
	if !ok {
		return nil, errors.New("A decoder does not exist for " + schema_name) 
	}
	
	for {
		record, err := this.Read(schema)
		
		if err == io.EOF {
			break
		} else if err != nil {	
			return nil, err
		}
		
		obj, err := fromRecord(record)
		if err != nil {
			return nil, err
		}
		
		objects = append(objects, obj)
	}
	
	return
}


func (this *AvroContainerReader) Close() {
	if this.reader != nil {
		C.avro_file_reader_close(this.reader)
		this.reader = nil
	}
}


type AvroContainerWriter struct {
	writer C.avro_file_writer_t
}

func NewAvroContainerBytesWriter(schema *AvroSchema) (writer *AvroContainerWriter, mem *MemStream, err error) {
	
	// create the writer
    mem = openMemstream()
    if mem == nil {
    	return nil, nil, errors.New("Couldn't allocate memory stream")
    }
	
	if writer, err = NewAvroFileWriter(mem.file, schema); err != nil {
		mem.Close()
	}
	
	return writer, mem, err
}

func NewAvroFileWriter(fp *C.FILE, schema *AvroSchema) (*AvroContainerWriter, error) {
	writer := C.my_avro_file_writer(fp, schema.schema)
	if writer == nil {
		return nil, errors.New("Creating writer: " + C.GoString(C.avro_strerror()))
	}
	return &AvroContainerWriter{writer}, nil
}


func (this *AvroContainerWriter) Close() {
	if this.writer != nil {
		C.avro_file_writer_close(this.writer)
		this.writer = nil
	}
}

func (this *AvroContainerWriter) Append(record *AvroRecord) error {
	
	// beware! if you pass in an invalid record, this may segfault
	
	// beware of https://issues.apache.org/jira/browse/AVRO-1558
	if C.avro_file_writer_append(this.writer, record.record) != 0 {
    	return errors.New("append record: " + C.GoString(C.avro_strerror()))
    }
	
	return nil
}

//
// Internal API
//

func avroGetFixed(datum C.avro_datum_t) ([]byte, error) {
	var clen C.int64_t 
	var cbytes *C.char
	
	if C.avro_fixed_get(datum, &cbytes, &clen) != 0 {
		return nil, errors.New("failed to get fixed element: " + C.GoString(C.avro_strerror()))
	}
	
	bytes := make([]byte, clen)
	
	// copy the bytes directly into the slice
	C.memcpy(unsafe.Pointer(&bytes[0]), unsafe.Pointer(cbytes), C.size_t(clen))
	
	return bytes, nil
}




//
// Finalizers
//

func finalizeSchema(this *AvroSchema) {
	C.avro_schema_decref(this.schema)
}

func finalizeRecord(this *AvroRecord) {
	C.avro_datum_decref(this.record)
}

func finalizeDatum(this *AvroDatum) {
	C.avro_datum_decref(this.datum)
}

func finalizeArray(this *AvroArray) {
	C.avro_datum_decref(this.datum)
}

func finalizeMap(this *AvroMap) {
	C.avro_datum_decref(this.datum)
}

]]#