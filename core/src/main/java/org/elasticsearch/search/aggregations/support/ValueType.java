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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;

import java.io.IOException;

/**
 *
 */
public enum ValueType implements Writeable<ValueType> {

    @Deprecated
    ANY((byte) 0, "any", ValuesSourceType.ANY, IndexFieldData.class, ValueFormat.RAW), STRING((byte) 1, "string", ValuesSourceType.BYTES,
            IndexFieldData.class,
            ValueFormat.RAW),
 LONG((byte) 2, "byte|short|integer|long", ValuesSourceType.NUMERIC,
            IndexNumericFieldData.class, ValueFormat.RAW) {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    DOUBLE((byte) 3, "float|double", ValuesSourceType.NUMERIC, IndexNumericFieldData.class, ValueFormat.RAW) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public boolean isFloatingPoint() {
            return true;
        }
    },
    NUMBER((byte) 4, "number", ValuesSourceType.NUMERIC, IndexNumericFieldData.class, ValueFormat.RAW) {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    DATE((byte) 5, "date", ValuesSourceType.NUMERIC, IndexNumericFieldData.class, ValueFormat.DateTime.DEFAULT) {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    IP((byte) 6, "ip", ValuesSourceType.NUMERIC, IndexNumericFieldData.class, ValueFormat.IPv4) {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    NUMERIC((byte) 7, "numeric", ValuesSourceType.NUMERIC, IndexNumericFieldData.class, ValueFormat.RAW) {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    GEOPOINT((byte) 8, "geo_point", ValuesSourceType.GEOPOINT, IndexGeoPointFieldData.class, ValueFormat.RAW) {
        @Override
        public boolean isGeoPoint() {
            return true;
        }
    };

    final String description;
    final ValuesSourceType valuesSourceType;
    final Class<? extends IndexFieldData> fieldDataType;
    final ValueFormat defaultFormat;
    private final byte id;

    private ValueType(byte id, String description, ValuesSourceType valuesSourceType, Class<? extends IndexFieldData> fieldDataType,
            ValueFormat defaultFormat) {
        this.id = id;
        this.description = description;
        this.valuesSourceType = valuesSourceType;
        this.fieldDataType = fieldDataType;
        this.defaultFormat = defaultFormat;
    }

    public String description() {
        return description;
    }

    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    public boolean compatibleWith(IndexFieldData fieldData) {
        return fieldDataType.isInstance(fieldData);
    }

    public boolean isA(ValueType valueType) {
        return valueType.valuesSourceType == valuesSourceType &&
                valueType.fieldDataType.isAssignableFrom(fieldDataType);
    }

    public boolean isNotA(ValueType valueType) {
        return !isA(valueType);
    }

    public ValueFormat defaultFormat() {
        return defaultFormat;
    }

    public boolean isNumeric() {
        return false;
    }

    public boolean isFloatingPoint() {
        return false;
    }

    public boolean isGeoPoint() {
        return false;
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
            default:
                return null;
        }
    }

    @Override
    public String toString() {
        return description;
    }

    @Override
    public ValueType readFrom(StreamInput in) throws IOException {
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
