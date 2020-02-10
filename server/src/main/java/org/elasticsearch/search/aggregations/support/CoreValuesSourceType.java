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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexHistogramFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.function.LongSupplier;

/**
 * {@link CoreValuesSourceType} holds the {@link ValuesSourceType} implementations for the core aggregations package.
 */
public enum CoreValuesSourceType implements ValuesSourceType {
    NUMERIC(EquivalenceType.NUMBER) {
        @Override
        public ValuesSource getEmpty() {
            return new ValuesSource.Numeric.Empty(NUMERIC);
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return new ValuesSource.Numeric.Script(NUMERIC, script, scriptValueType);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {

            return getNumericFieldValuesSource(fieldContext, script, NUMERIC);
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            Number missing = docValueFormat.parseDouble(rawMissing.toString(), false, now);
            return MissingValues.replaceMissing((ValuesSource.Numeric) valuesSource, missing);
        }
    },
    BYTES(EquivalenceType.STRING) {
        @Override
        public ValuesSource getEmpty() {
            return new ValuesSource.Bytes.WithOrdinals.Empty(BYTES);
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return new ValuesSource.Bytes.Script(BYTES, script);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            return getBytesFieldValuesSource(fieldContext, script, BYTES);
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            final BytesRef missing = docValueFormat.parseBytesRef(rawMissing.toString());
            if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
                return MissingValues.replaceMissing((ValuesSource.Bytes.WithOrdinals) valuesSource, missing);
            } else {
                return MissingValues.replaceMissing((ValuesSource.Bytes) valuesSource, missing);
            }
        }
    },
    GEOPOINT(EquivalenceType.GEO) {
        @Override
        public ValuesSource getEmpty() {
            return new  ValuesSource.GeoPoint.Empty(GEOPOINT);
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw new AggregationExecutionException("value source of type [" + this.value() + "] is not supported by scripts");
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            if (!(fieldContext.indexFieldData() instanceof IndexGeoPointFieldData)) {
                // TODO: Is this the correct exception type here?
                throw new IllegalArgumentException("Expected geo_point type on field [" + fieldContext.field() +
                    "], but got [" + fieldContext.fieldType().typeName() + "]");
            }

            return new ValuesSource.GeoPoint.Fielddata(GEOPOINT, (IndexGeoPointFieldData) fieldContext.indexFieldData());
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            // TODO: also support the structured formats of geo points
            final GeoPoint missing = new GeoPoint(rawMissing.toString());
            return MissingValues.replaceMissing((ValuesSource.GeoPoint) valuesSource, missing);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            return DocValueFormat.GEOHASH;
        }
    },
    RANGE(EquivalenceType.RANGE) {
        @Override
        public ValuesSource getEmpty() {
            // TODO: Is this the correct exception type here?
            throw new IllegalArgumentException("Can't deal with unmapped ValuesSource type " + this.value());
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw new AggregationExecutionException("value source of type [" + this.value() + "] is not supported by scripts");
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            MappedFieldType fieldType = fieldContext.fieldType();

            if (fieldType instanceof RangeFieldMapper.RangeFieldType == false) {
                // TODO: Is this the correct exception type here?
                throw new IllegalStateException("Asked for range ValuesSource, but field is of type " + fieldType.name());
            }
            RangeFieldMapper.RangeFieldType rangeFieldType = (RangeFieldMapper.RangeFieldType) fieldType;
            return new ValuesSource.Range(RANGE, fieldContext.indexFieldData(), rangeFieldType.rangeType());
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            throw new IllegalArgumentException("Can't apply missing values on a " + valuesSource.getClass());
        }
    },
    HISTOGRAM(EquivalenceType.HISTOGRAM) {
        @Override
        public ValuesSource getEmpty() {
            // TODO: Is this the correct exception type here?
            throw new IllegalArgumentException("Can't deal with unmapped ValuesSource type " + this.value());
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw new AggregationExecutionException("value source of type [" + this.value() + "] is not supported by scripts");
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            final IndexFieldData<?> indexFieldData = fieldContext.indexFieldData();

            if (!(indexFieldData instanceof IndexHistogramFieldData)) {
                throw new IllegalArgumentException("Expected histogram type on field [" + fieldContext.field() +
                    "], but got [" + fieldContext.fieldType().typeName() + "]");
            }
            return new ValuesSource.Histogram.Fielddata(HISTOGRAM, (IndexHistogramFieldData) indexFieldData);
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            throw new IllegalArgumentException("Can't apply missing values on a " + valuesSource.getClass());
        }
    },
    IP(EquivalenceType.STRING) {
        @Override
        public ValuesSource getEmpty() {
            return new ValuesSource.Bytes.WithOrdinals.Empty(IP);
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return new ValuesSource.Bytes.Script(IP, script);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            return getBytesFieldValuesSource(fieldContext, script, IP);
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            return BYTES.replaceMissing(valuesSource, rawMissing, docValueFormat, now);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            return DocValueFormat.IP;
        }
    },
    DATE(EquivalenceType.NUMBER) {
        @Override
        public ValuesSource getEmpty() {
            return new ValuesSource.Numeric.Empty(DATE);
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return new ValuesSource.Numeric.Script(DATE, script, scriptValueType);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            return getNumericFieldValuesSource(fieldContext, script, DATE);
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            return NUMERIC.replaceMissing(valuesSource, rawMissing, docValueFormat, now);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            return  new DocValueFormat.DateTime(
                format == null ? DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER : DateFormatter.forPattern(format),
                tz == null ? ZoneOffset.UTC : tz,
                // If we were just looking at fields, we could read the resolution from the field settings, but we need to deal with script
                // output, which has no way to indicate the resolution, so we need to default to something.  Milliseconds is the standard.
                DateFieldMapper.Resolution.MILLISECONDS);
        }
    },
    BOOLEAN(EquivalenceType.NUMBER) {
        @Override
        public ValuesSource getEmpty() {
            return new ValuesSource.Numeric.Empty(BOOLEAN);
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return new ValuesSource.Numeric.Script(BOOLEAN, script, scriptValueType);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            return getNumericFieldValuesSource(fieldContext, script, BOOLEAN);
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            return NUMERIC.replaceMissing(valuesSource, rawMissing, docValueFormat, now);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            return DocValueFormat.BOOLEAN;
        }
    }
    ;

    enum EquivalenceType {
        STRING, NUMBER, GEO, RANGE, HISTOGRAM;
    }

    EquivalenceType equivalenceType;

    CoreValuesSourceType(EquivalenceType equivalenceType) {
        this.equivalenceType = equivalenceType;
    }
    @Override
    public boolean isCastableTo(ValuesSourceType valuesSourceType) {
        if (valuesSourceType instanceof CoreValuesSourceType == false) {
            return false;
        }
        CoreValuesSourceType other = (CoreValuesSourceType) valuesSourceType;
        return this.equivalenceType == other.equivalenceType;
    }

    public static ValuesSourceType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Helper method for BYTES and IP, which have the same logic for constructing {@link ValuesSource} instances, but need to specify
     * different {@link ValuesSourceType}s
     */
    private static ValuesSource getBytesFieldValuesSource(FieldContext fieldContext, AggregationScript.LeafFactory script,
                                                          CoreValuesSourceType valuesSourceType) {
        final IndexFieldData<?> indexFieldData = fieldContext.indexFieldData();
        ValuesSource dataSource;
        if (indexFieldData instanceof IndexOrdinalsFieldData) {
            dataSource = new ValuesSource.Bytes.WithOrdinals.FieldData(valuesSourceType, (IndexOrdinalsFieldData) indexFieldData);
        } else {
            dataSource = new ValuesSource.Bytes.FieldData(valuesSourceType, indexFieldData);
        }
        if (script != null) {
            dataSource = new ValuesSource.Bytes.WithScript(dataSource, script);
        }
        return dataSource;
    }

    private static ValuesSource getNumericFieldValuesSource(FieldContext fieldContext, AggregationScript.LeafFactory script,
                                                            CoreValuesSourceType valuesSourceType) {
        if ((fieldContext.indexFieldData() instanceof IndexNumericFieldData) == false) {
            // TODO: Is this the correct exception type here?
            throw new IllegalArgumentException("Expected numeric type on field [" + fieldContext.field() +
                "], but got [" + fieldContext.fieldType().typeName() + "]");
        }

        ValuesSource.Numeric dataSource = new ValuesSource.Numeric.FieldData(valuesSourceType,
            (IndexNumericFieldData) fieldContext.indexFieldData());
        if (script != null) {
            // Value script case
            dataSource = new ValuesSource.Numeric.WithScript(dataSource, script);
        }
        return dataSource;
    }
}
