/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Set;

/**
 * @deprecated We are in the process of replacing this class with {@link ValuesSourceType}, so new uses or entries to the enum are
 * discouraged.  There is currently no migration path for existing uses, notably parsing user value type hints and Composite aggregation,
 * should continue to use this for now. Most importantly DO NOT ADD NEW PLACES WE SERIALIZE THIS ENUM!
 */
@Deprecated
public enum ValueType implements Writeable {

    STRING((byte) 1, "string", "string", CoreValuesSourceType.KEYWORD,
        DocValueFormat.RAW),

    LONG((byte) 2, "byte|short|integer|long", "long", CoreValuesSourceType.NUMERIC, DocValueFormat.RAW),
    DOUBLE((byte) 3, "float|double", "double", CoreValuesSourceType.NUMERIC, DocValueFormat.RAW),
    NUMBER((byte) 4, "number", "number", CoreValuesSourceType.NUMERIC, DocValueFormat.RAW),
    DATE((byte) 5, "date", "date", CoreValuesSourceType.DATE,
        new DocValueFormat.DateTime(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER, ZoneOffset.UTC,
                DateFieldMapper.Resolution.MILLISECONDS)),
    IP((byte) 6, "ip", "ip", CoreValuesSourceType.IP, DocValueFormat.IP),
    NUMERIC((byte) 7, "numeric", "numeric", CoreValuesSourceType.NUMERIC, DocValueFormat.RAW),
    GEOPOINT((byte) 8, "geo_point", "geo_point", CoreValuesSourceType.GEOPOINT, DocValueFormat.GEOHASH),
    BOOLEAN((byte) 9, "boolean", "boolean", CoreValuesSourceType.BOOLEAN, DocValueFormat.BOOLEAN),
    RANGE((byte) 10, "range", "range", CoreValuesSourceType.RANGE, DocValueFormat.RAW);

    final String description;
    final ValuesSourceType valuesSourceType;
    final DocValueFormat defaultFormat;
    private final byte id;
    private String preferredName;

    public static final ParseField VALUE_TYPE = new ParseField("value_type", "valueType");

    ValueType(byte id, String description, String preferredName, ValuesSourceType valuesSourceType,
              DocValueFormat defaultFormat) {
        this.id = id;
        this.description = description;
        this.preferredName = preferredName;
        this.valuesSourceType = valuesSourceType;
        this.defaultFormat = defaultFormat;
    }

    public String getPreferredName() {
        return preferredName;
    }

    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    private static Set<ValueType> numericValueTypes = Set.of(ValueType.DOUBLE, ValueType.DATE, ValueType.LONG, ValueType.NUMBER,
        ValueType.NUMERIC, ValueType.BOOLEAN);
    private static Set<ValueType> stringValueTypes = Set.of(ValueType.STRING, ValueType.IP);

    /**
     * This is a bit of a hack to mirror the old {@link ValueType} behavior, which would allow a rough compatibility between types.  This
     * behavior is being phased out in the aggregations framework, in favor of explicitly listing supported types, but we haven't gotten
     * to fixing composite yet.
     *
     * @param valueType The value type the user suggested
     * @return True iff the two value types are interchangeable
     */
    public boolean isA(ValueType valueType) {
        if (numericValueTypes.contains(this)) {
            return numericValueTypes.contains(valueType);
        }
        if (stringValueTypes.contains(this)) {
            return stringValueTypes.contains(valueType);
        }
        return this.equals(valueType);
    }

    public boolean isNotA(ValueType valueType) {
        return isA(valueType) == false;
    }

    public DocValueFormat defaultFormat() {
        return defaultFormat;
    }

    public static ValueType lenientParse(String type) {
        switch (type) {
            case "string":  return STRING;
            case "double":
            case "float":   return DOUBLE;
            case "number":
            case "numeric":
            case "long":
            case "integer":
            case "short":
            case "byte":    return LONG;
            case "date":    return DATE;
            case "ip":      return IP;
            case "boolean": return BOOLEAN;
            default:
                // TODO: do not be lenient here
                return null;
        }
    }

    @Override
    public String toString() {
        return description;
    }

    public static ValueType readFromStream(StreamInput in) throws IOException {
        byte id = in.readByte();
        for (ValueType valueType : values()) {
            if (id == valueType.id) {
                return valueType;
            }
        }
        throw new IOException("No ValueType found for id [" + id + "]");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(id);
    }
}
