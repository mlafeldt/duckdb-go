package duckdb

import (
	"fmt"
	"math"
	"reflect"
)

// TypedValue wraps a Go value with an explicit DuckDB type for parameter
// binding. Use it when DuckDB cannot infer the parameter type from SQL alone,
// or when the default Go-type inference would choose a different DuckDB type.
//
// TypedValue is intentionally a narrow binding hint rather than a general
// conversion API. It selects the DuckDB parameter type while preserving the
// usual Go value semantics for that type.
//
// Typed parameters support scalar types that can be represented by a bare Type
// enum. Types that need extra logical type metadata, such as DECIMAL, ENUM,
// LIST, ARRAY, STRUCT, MAP, and UNION are rejected. UUID, BIT, BLOB, HUGEINT,
// UHUGEINT, and BIGNUM are also outside the Typed scope for now.
//
// If the type is TYPE_SQLNULL, the value is ignored without calling
// driver.Valuer and the parameter is bound as SQL NULL. A nil value, including
// a nil *TypedValue argument, also binds SQL NULL.
//
// Coercions are intentionally narrow: TYPE_BOOLEAN accepts only bool,
// TYPE_VARCHAR accepts only string. Integer types accept Go integer values in
// range. Floating-point types accept only Go float32 or float64 values.
// TYPE_FLOAT rejects finite float64 values that would overflow or underflow to
// zero as float32. NaN and infinities are preserved.
type TypedValue struct {
	value any
	typ   Type
}

// Typed returns a TypedValue for explicit DuckDB parameter binding. Validation
// happens when the value is bound, so Typed remains a lightweight wrapper with
// the same call shape for every supported target type.
func Typed(value any, typ Type) TypedValue {
	return TypedValue{
		value: value,
		typ:   typ,
	}
}

func coerceTypedValue(t Type, v any) (any, error) {
	if !supportsTypedValue(t) {
		return nil, unsupportedTypeError(typedValueTypeName(t))
	}

	if t == TYPE_SQLNULL || isNil(v) {
		return nil, nil
	}

	switch t {
	case TYPE_BOOLEAN:
		if _, ok := v.(bool); ok {
			return v, nil
		}
		return nil, typedValueCastError(t, v)
	case TYPE_TINYINT:
		i, err := coerceTypedSignedInteger(t, v, math.MinInt8, math.MaxInt8)
		return int8(i), err
	case TYPE_SMALLINT:
		i, err := coerceTypedSignedInteger(t, v, math.MinInt16, math.MaxInt16)
		return int16(i), err
	case TYPE_INTEGER:
		i, err := coerceTypedSignedInteger(t, v, math.MinInt32, math.MaxInt32)
		return int32(i), err
	case TYPE_BIGINT:
		return coerceTypedSignedInteger(t, v, math.MinInt64, math.MaxInt64)
	case TYPE_UTINYINT:
		i, err := coerceTypedUnsignedInteger(t, v, math.MaxUint8)
		return uint8(i), err
	case TYPE_USMALLINT:
		i, err := coerceTypedUnsignedInteger(t, v, math.MaxUint16)
		return uint16(i), err
	case TYPE_UINTEGER:
		i, err := coerceTypedUnsignedInteger(t, v, math.MaxUint32)
		return uint32(i), err
	case TYPE_UBIGINT:
		return coerceTypedUnsignedInteger(t, v, math.MaxUint64)
	case TYPE_FLOAT:
		return coerceTypedFloat32(t, v)
	case TYPE_DOUBLE:
		return coerceTypedFloat64(t, v)
	case TYPE_VARCHAR:
		if _, ok := v.(string); ok {
			return v, nil
		}
		return nil, typedValueCastError(t, v)
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS,
		TYPE_DATE, TYPE_TIME, TYPE_TIME_TZ, TYPE_INTERVAL:
		return v, nil
	}
	return nil, fmt.Errorf("duckdb: internal error: missing typed value coercion for %s", typedValueTypeName(t))
}

func supportsTypedValue(t Type) bool {
	switch t {
	case TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INTEGER, TYPE_BIGINT,
		TYPE_UTINYINT, TYPE_USMALLINT, TYPE_UINTEGER, TYPE_UBIGINT,
		TYPE_FLOAT, TYPE_DOUBLE, TYPE_VARCHAR,
		TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS,
		TYPE_DATE, TYPE_TIME, TYPE_TIME_TZ, TYPE_INTERVAL,
		TYPE_SQLNULL:
		return true
	default:
		return false
	}
}

func coerceTypedSignedInteger(t Type, v any, min, max int64) (int64, error) {
	value := reflect.ValueOf(v)
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return coerceTypedSignedRange(t, value.Int(), min, max)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return coerceTypedUnsignedToSigned(t, value.Uint(), max)
	default:
		return 0, typedValueCastError(t, v)
	}
}

func coerceTypedSignedRange(t Type, v, min, max int64) (int64, error) {
	if v < min || v > max {
		return 0, typedValueConversionError(t, v)
	}
	return v, nil
}

func coerceTypedUnsignedToSigned(t Type, v uint64, max int64) (int64, error) {
	if v > uint64(max) {
		return 0, typedValueConversionError(t, v)
	}
	return int64(v), nil
}

func coerceTypedUnsignedInteger(t Type, v any, max uint64) (uint64, error) {
	value := reflect.ValueOf(v)
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return coerceTypedSignedToUnsigned(t, value.Int(), max)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return coerceTypedUnsignedRange(t, value.Uint(), max)
	default:
		return 0, typedValueCastError(t, v)
	}
}

func coerceTypedSignedToUnsigned(t Type, v int64, max uint64) (uint64, error) {
	if v < 0 || uint64(v) > max {
		return 0, typedValueConversionError(t, v)
	}
	return uint64(v), nil
}

func coerceTypedUnsignedRange(t Type, v, max uint64) (uint64, error) {
	if v > max {
		return 0, typedValueConversionError(t, v)
	}
	return v, nil
}

func coerceTypedFloat32(t Type, v any) (float32, error) {
	switch vv := v.(type) {
	case float32:
		return vv, nil
	case float64:
		// Avoid silently converting finite float64 values to float32 infinities or zero.
		if !math.IsInf(vv, 0) && math.Abs(vv) > math.MaxFloat32 {
			return 0, typedValueConversionError(t, v)
		}
		if vv != 0 && float32(vv) == 0 {
			return 0, typedValueConversionError(t, v)
		}
		return float32(vv), nil
	default:
		return 0, typedValueCastError(t, v)
	}
}

func coerceTypedFloat64(t Type, v any) (float64, error) {
	switch vv := v.(type) {
	case float32:
		return float64(vv), nil
	case float64:
		return vv, nil
	default:
		return 0, typedValueCastError(t, v)
	}
}

func typedValueCastError(t Type, v any) error {
	return castErrorForValue(v, typedValueTypeName(t))
}

func typedValueConversionError(t Type, v any) error {
	return fmt.Errorf("%s: cannot convert %v to %s", convertErrMsg, v, typedValueTypeName(t))
}

func typedValueTypeName(t Type) string {
	name, ok := typeToStringMap[t]
	if !ok {
		return unknownTypeErrMsg
	}
	return name
}
