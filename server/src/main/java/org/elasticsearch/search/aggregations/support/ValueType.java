/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.BinaryDVIndexFieldData;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.time.ZoneOffset;

public enum ValueType implements Writeable {

    STRING((byte) 1, "string", "string", CoreValuesSourceType.BYTES,
            IndexFieldData.class, DocValueFormat.RAW),
    LONG((byte) 2, "byte|short|integer|long", "long",
                    CoreValuesSourceType.NUMERIC,
            IndexNumericFieldData.class, DocValueFormat.RAW),
    DOUBLE((byte) 3, "float|double", "double", CoreValuesSourceType.NUMERIC, IndexNumericFieldData.class, DocValueFormat.RAW),
    NUMBER((byte) 4, "number", "number", CoreValuesSourceType.NUMERIC, IndexNumericFieldData.class, DocValueFormat.RAW),
    DATE((byte) 5, "date", "date", CoreValuesSourceType.NUMERIC, IndexNumericFieldData.class,
            new DocValueFormat.DateTime(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER, ZoneOffset.UTC,
                DateFieldMapper.Resolution.MILLISECONDS)),
    IP((byte) 6, "ip", "ip", CoreValuesSourceType.BYTES, IndexFieldData.class, DocValueFormat.IP),
    // TODO: what is the difference between "number" and "numeric"?
    NUMERIC((byte) 7, "numeric", "numeric", CoreValuesSourceType.NUMERIC, IndexNumericFieldData.class, DocValueFormat.RAW),
    GEOPOINT((byte) 8, "geo_point", "geo_point", CoreValuesSourceType.GEOPOINT, IndexGeoPointFieldData.class, DocValueFormat.GEOHASH),
    BOOLEAN((byte) 9, "boolean", "boolean", CoreValuesSourceType.NUMERIC, IndexNumericFieldData.class, DocValueFormat.BOOLEAN),
    RANGE((byte) 10, "range", "range", CoreValuesSourceType.RANGE, BinaryDVIndexFieldData.class, DocValueFormat.RAW);

    final String description;
    final ValuesSourceType valuesSourceType;
    final Class<? extends IndexFieldData> fieldDataType;
    final DocValueFormat defaultFormat;
    private final byte id;
    private String preferredName;

    public static final ParseField VALUE_TYPE = new ParseField("value_type", "valueType");

    ValueType(byte id, String description, String preferredName, ValuesSourceType valuesSourceType,
            Class<? extends IndexFieldData> fieldDataType, DocValueFormat defaultFormat) {
        this.id = id;
        this.description = description;
        this.preferredName = preferredName;
        this.valuesSourceType = valuesSourceType;
        this.fieldDataType = fieldDataType;
        this.defaultFormat = defaultFormat;
    }

    public String getPreferredName() {
        return preferredName;
    }

    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    public boolean isA(ValueType valueType) {
        return valueType.valuesSourceType == valuesSourceType &&
                valueType.fieldDataType.isAssignableFrom(fieldDataType);
    }

    public boolean isNotA(ValueType valueType) {
        return !isA(valueType);
    }

    public DocValueFormat defaultFormat() {
        return defaultFormat;
    }

    public static ValueType resolveForScript(String type) {
        switch (type) {
            case "string":  return STRING;
            case "double":
            case "float":   return DOUBLE;
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
        throw new IOException("No valueType found for id [" + id + "]");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(id);
    }
}
