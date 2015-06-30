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

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;

/**
 *
 */
public enum ValueType {

    @Deprecated
    ANY("any", ValuesSource.class, IndexFieldData.class, ValueFormat.RAW), STRING("string", ValuesSource.Bytes.class, IndexFieldData.class,
            ValueFormat.RAW),
    LONG("byte|short|integer|long", ValuesSource.Numeric.class, IndexNumericFieldData.class, ValueFormat.RAW) {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    DOUBLE("float|double", ValuesSource.Numeric.class, IndexNumericFieldData.class, ValueFormat.RAW) {
        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public boolean isFloatingPoint() {
            return true;
        }
    },
    NUMBER("number", ValuesSource.Numeric.class, IndexNumericFieldData.class, ValueFormat.RAW) {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    DATE("date", ValuesSource.Numeric.class, IndexNumericFieldData.class, ValueFormat.DateTime.DEFAULT) {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    IP("ip", ValuesSource.Numeric.class, IndexNumericFieldData.class, ValueFormat.IPv4) {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    NUMERIC("numeric", ValuesSource.Numeric.class, IndexNumericFieldData.class, ValueFormat.RAW) {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    GEOPOINT("geo_point", ValuesSource.GeoPoint.class, IndexGeoPointFieldData.class, ValueFormat.RAW) {
        @Override
        public boolean isGeoPoint() {
            return true;
        }
    };

    final String description;
    final Class<? extends ValuesSource> valuesSourceType;
    final Class<? extends IndexFieldData> fieldDataType;
    final ValueFormat defaultFormat;

    private ValueType(String description, Class<? extends ValuesSource> valuesSourceType, Class<? extends IndexFieldData> fieldDataType, ValueFormat defaultFormat) {
        this.description = description;
        this.valuesSourceType = valuesSourceType;
        this.fieldDataType = fieldDataType;
        this.defaultFormat = defaultFormat;
    }

    public String description() {
        return description;
    }

    public Class<? extends ValuesSource> getValuesSourceType() {
        return valuesSourceType;
    }

    public boolean compatibleWith(IndexFieldData fieldData) {
        return fieldDataType.isInstance(fieldData);
    }

    public boolean isA(ValueType valueType) {
        return valueType.valuesSourceType.isAssignableFrom(valuesSourceType) &&
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
}
