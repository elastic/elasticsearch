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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.HalfFloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A {@link FieldMapper} for numeric types: byte, short, int, long, float and double. */
public class NumberFieldMapper extends FieldMapper {

    // this is private since it has a different default
    static final Setting<Boolean> COERCE_SETTING =
            Setting.boolSetting("index.mapping.coerce", true, Property.IndexScope);

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Boolean> COERCE = new Explicit<>(true, false);
    }

    public static class Builder extends FieldMapper.Builder<Builder, NumberFieldMapper> {

        private Boolean ignoreMalformed;
        private Boolean coerce;

        public Builder(String name, NumberType type) {
            super(name, new NumberFieldType(type), new NumberFieldType(type));
            builder = this;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.IGNORE_MALFORMED;
        }

        public Builder coerce(boolean coerce) {
            this.coerce = coerce;
            return builder;
        }

        protected Explicit<Boolean> coerce(BuilderContext context) {
            if (coerce != null) {
                return new Explicit<>(coerce, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(COERCE_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.COERCE;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
        }

        @Override
        public NumberFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new NumberFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context),
                    coerce(context), includeInAll, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        final NumberType type;

        public TypeParser(NumberType type) {
            this.type = type;
        }

        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node,
                                         ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name, type);
            TypeParsers.parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(type.parse(propNode, false));
                    iterator.remove();
                } else if (propName.equals("ignore_malformed")) {
                    builder.ignoreMalformed(TypeParsers.nodeBooleanValue(name,"ignore_malformed", propNode, parserContext));
                    iterator.remove();
                } else if (propName.equals("coerce")) {
                    builder.coerce(TypeParsers.nodeBooleanValue(name, "coerce", propNode, parserContext));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public enum NumberType {
        HALF_FLOAT("half_float", NumericType.HALF_FLOAT) {
            @Override
            Float parse(Object value, boolean coerce) {
                return (Float) FLOAT.parse(value, false);
            }

            @Override
            Float parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.floatValue(coerce);
            }

            @Override
            Query termQuery(String field, Object value) {
                float v = parse(value, false);
                return HalfFloatPoint.newExactQuery(field, v);
            }

            @Override
            Query termsQuery(String field, List<Object> values) {
                float[] v = new float[values.size()];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i), false);
                }
                return HalfFloatPoint.newSetQuery(field, v);
            }

            @Override
            Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                float l = Float.NEGATIVE_INFINITY;
                float u = Float.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, false);
                    if (includeLower) {
                        l = HalfFloatPoint.nextDown(l);
                    }
                    l = HalfFloatPoint.nextUp(l);
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, false);
                    if (includeUpper) {
                        u = HalfFloatPoint.nextUp(u);
                    }
                    u = HalfFloatPoint.nextDown(u);
                }
                Query query = HalfFloatPoint.newRangeQuery(field, l, u);
                if (hasDocValues) {
                    Query dvQuery = SortedNumericDocValuesField.newRangeQuery(field,
                            HalfFloatPoint.halfFloatToSortableShort(l),
                            HalfFloatPoint.halfFloatToSortableShort(u));
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                }
                return query;
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new HalfFloatPoint(name, value.floatValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name,
                        HalfFloatPoint.halfFloatToSortableShort(value.floatValue())));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.floatValue()));
                }
                return fields;
            }

            @Override
            FieldStats.Double stats(IndexReader reader, String fieldName,
                                    boolean isSearchable, boolean isAggregatable) throws IOException {
                FieldInfo fi = org.apache.lucene.index.MultiFields.getMergedFieldInfos(reader).fieldInfo(fieldName);
                if (fi == null) {
                    return null;
                }
                long size = PointValues.size(reader, fieldName);
                if (size == 0) {
                    return new FieldStats.Double(reader.maxDoc(), 0, -1, -1, isSearchable, isAggregatable);
                }
                int docCount = PointValues.getDocCount(reader, fieldName);
                byte[] min = PointValues.getMinPackedValue(reader, fieldName);
                byte[] max = PointValues.getMaxPackedValue(reader, fieldName);
                return new FieldStats.Double(reader.maxDoc(), docCount, -1L, size,
                    isSearchable, isAggregatable,
                    HalfFloatPoint.decodeDimension(min, 0), HalfFloatPoint.decodeDimension(max, 0));
            }
        },
        FLOAT("float", NumericType.FLOAT) {
            @Override
            Float parse(Object value, boolean coerce) {
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                return Float.parseFloat(value.toString());
            }

            @Override
            Float parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.floatValue(coerce);
            }

            @Override
            Query termQuery(String field, Object value) {
                float v = parse(value, false);
                return FloatPoint.newExactQuery(field, v);
            }

            @Override
            Query termsQuery(String field, List<Object> values) {
                float[] v = new float[values.size()];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i), false);
                }
                return FloatPoint.newSetQuery(field, v);
            }

            @Override
            Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                float l = Float.NEGATIVE_INFINITY;
                float u = Float.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, false);
                    if (includeLower == false) {
                        l = FloatPoint.nextUp(l);
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, false);
                    if (includeUpper == false) {
                        u = FloatPoint.nextDown(u);
                    }
                }
                Query query = FloatPoint.newRangeQuery(field, l, u);
                if (hasDocValues) {
                    Query dvQuery = SortedNumericDocValuesField.newRangeQuery(field,
                            NumericUtils.floatToSortableInt(l),
                            NumericUtils.floatToSortableInt(u));
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                }
                return query;
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new FloatPoint(name, value.floatValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name,
                        NumericUtils.floatToSortableInt(value.floatValue())));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.floatValue()));
                }
                return fields;
            }

            @Override
            FieldStats.Double stats(IndexReader reader, String fieldName,
                                    boolean isSearchable, boolean isAggregatable) throws IOException {
                FieldInfo fi = org.apache.lucene.index.MultiFields.getMergedFieldInfos(reader).fieldInfo(fieldName);
                if (fi == null) {
                    return null;
                }
                long size = PointValues.size(reader, fieldName);
                if (size == 0) {
                    return new FieldStats.Double(reader.maxDoc(), 0, -1, -1, isSearchable, isAggregatable);
                }
                int docCount = PointValues.getDocCount(reader, fieldName);
                byte[] min = PointValues.getMinPackedValue(reader, fieldName);
                byte[] max = PointValues.getMaxPackedValue(reader, fieldName);
                return new FieldStats.Double(reader.maxDoc(),docCount, -1L, size,
                    isSearchable, isAggregatable,
                    FloatPoint.decodeDimension(min, 0), FloatPoint.decodeDimension(max, 0));
            }
        },
        DOUBLE("double", NumericType.DOUBLE) {
            @Override
            Double parse(Object value, boolean coerce) {
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                return Double.parseDouble(value.toString());
            }

            @Override
            Double parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.doubleValue(coerce);
            }

            @Override
            Query termQuery(String field, Object value) {
                double v = parse(value, false);
                return DoublePoint.newExactQuery(field, v);
            }

            @Override
            Query termsQuery(String field, List<Object> values) {
                double[] v = new double[values.size()];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i), false);
                }
                return DoublePoint.newSetQuery(field, v);
            }

            @Override
            Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                double l = Double.NEGATIVE_INFINITY;
                double u = Double.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, false);
                    if (includeLower == false) {
                        l = DoublePoint.nextUp(l);
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, false);
                    if (includeUpper == false) {
                        u = DoublePoint.nextDown(u);
                    }
                }
                Query query = DoublePoint.newRangeQuery(field, l, u);
                if (hasDocValues) {
                    Query dvQuery = SortedNumericDocValuesField.newRangeQuery(field,
                            NumericUtils.doubleToSortableLong(l),
                            NumericUtils.doubleToSortableLong(u));
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                }
                return query;
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new DoublePoint(name, value.doubleValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name,
                        NumericUtils.doubleToSortableLong(value.doubleValue())));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.doubleValue()));
                }
                return fields;
            }

            @Override
            FieldStats.Double stats(IndexReader reader, String fieldName,
                                    boolean isSearchable, boolean isAggregatable) throws IOException {
                FieldInfo fi = org.apache.lucene.index.MultiFields.getMergedFieldInfos(reader).fieldInfo(fieldName);
                if (fi == null) {
                    return null;
                }
                long size = PointValues.size(reader, fieldName);
                if (size == 0) {
                    return new FieldStats.Double(reader.maxDoc(),0, -1, -1, isSearchable, isAggregatable);
                }
                int docCount = PointValues.getDocCount(reader, fieldName);
                byte[] min = PointValues.getMinPackedValue(reader, fieldName);
                byte[] max = PointValues.getMaxPackedValue(reader, fieldName);
                return new FieldStats.Double(reader.maxDoc(),docCount, -1L, size,
                    isSearchable, isAggregatable,
                    DoublePoint.decodeDimension(min, 0), DoublePoint.decodeDimension(max, 0));
            }
        },
        BYTE("byte", NumericType.BYTE) {
            @Override
            Byte parse(Object value, boolean coerce) {
                if (value instanceof Number) {
                    double doubleValue = ((Number) value).doubleValue();
                    if (doubleValue < Byte.MIN_VALUE || doubleValue > Byte.MAX_VALUE) {
                        throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
                    }
                    if (!coerce && doubleValue % 1 != 0) {
                        throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                    }
                    return ((Number) value).byteValue();
                }
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                return Byte.parseByte(value.toString());
            }

            @Override
            Short parse(XContentParser parser, boolean coerce) throws IOException {
                int value = parser.intValue(coerce);
                if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
                }
                return (short) value;
            }

            @Override
            Query termQuery(String field, Object value) {
                return INTEGER.termQuery(field, value);
            }

            @Override
            Query termsQuery(String field, List<Object> values) {
                return INTEGER.termsQuery(field, values);
            }

            @Override
            Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                return INTEGER.rangeQuery(field, lowerTerm, upperTerm, includeLower, includeUpper, hasDocValues);
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                return INTEGER.createFields(name, value, indexed, docValued, stored);
            }

            @Override
            FieldStats.Long stats(IndexReader reader, String fieldName,
                                  boolean isSearchable, boolean isAggregatable) throws IOException {
                return (FieldStats.Long) INTEGER.stats(reader, fieldName, isSearchable, isAggregatable);
            }

            @Override
            Number valueForSearch(Number value) {
                return value.byteValue();
            }
        },
        SHORT("short", NumericType.SHORT) {
            @Override
            Short parse(Object value, boolean coerce) {
                if (value instanceof Number) {
                    double doubleValue = ((Number) value).doubleValue();
                    if (doubleValue < Short.MIN_VALUE || doubleValue > Short.MAX_VALUE) {
                        throw new IllegalArgumentException("Value [" + value + "] is out of range for a short");
                    }
                    if (!coerce && doubleValue % 1 != 0) {
                        throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                    }
                    return ((Number) value).shortValue();
                }
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                return Short.parseShort(value.toString());
            }

            @Override
            Short parse(XContentParser parser, boolean coerce) throws IOException {
                int value = parser.intValue(coerce);
                if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a short");
                }
                return (short) value;
            }

            @Override
            Query termQuery(String field, Object value) {
                return INTEGER.termQuery(field, value);
            }

            @Override
            Query termsQuery(String field, List<Object> values) {
                return INTEGER.termsQuery(field, values);
            }

            @Override
            Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                return INTEGER.rangeQuery(field, lowerTerm, upperTerm, includeLower, includeUpper, hasDocValues);
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                return INTEGER.createFields(name, value, indexed, docValued, stored);
            }

            @Override
            FieldStats.Long stats(IndexReader reader, String fieldName,
                                  boolean isSearchable, boolean isAggregatable) throws IOException {
                return (FieldStats.Long) INTEGER.stats(reader, fieldName, isSearchable, isAggregatable);
            }

            @Override
            Number valueForSearch(Number value) {
                return value.shortValue();
            }
        },
        INTEGER("integer", NumericType.INT) {
            @Override
            Integer parse(Object value, boolean coerce) {
                if (value instanceof Number) {
                    double doubleValue = ((Number) value).doubleValue();
                    if (doubleValue < Integer.MIN_VALUE || doubleValue > Integer.MAX_VALUE) {
                        throw new IllegalArgumentException("Value [" + value + "] is out of range for an integer");
                    }
                    if (!coerce && doubleValue % 1 != 0) {
                        throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                    }
                    return ((Number) value).intValue();
                }
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                return Integer.parseInt(value.toString());
            }

            @Override
            Integer parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.intValue(coerce);
            }

            @Override
            Query termQuery(String field, Object value) {
                if (hasDecimalPart(value)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
                }
                int v = parse(value, true);
                return IntPoint.newExactQuery(field, v);
            }

            @Override
            Query termsQuery(String field, List<Object> values) {
                int[] v = new int[values.size()];
                int upTo = 0;

                for (int i = 0; i < values.size(); i++) {
                    Object value = values.get(i);
                    if (!hasDecimalPart(value)) {
                        v[upTo++] = parse(value, true);
                    }
                }

                if (upTo == 0) {
                    return Queries.newMatchNoDocsQuery("All values have a decimal part");
                }
                if (upTo != v.length) {
                    v = Arrays.copyOf(v, upTo);
                }
                return IntPoint.newSetQuery(field, v);
            }

            @Override
            Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                int l = Integer.MIN_VALUE;
                int u = Integer.MAX_VALUE;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, true);
                    // if the lower bound is decimal:
                    // - if the bound is positive then we increment it:
                    //      if lowerTerm=1.5 then the (inclusive) bound becomes 2
                    // - if the bound is negative then we leave it as is:
                    //      if lowerTerm=-1.5 then the (inclusive) bound becomes -1 due to the call to longValue
                    boolean lowerTermHasDecimalPart = hasDecimalPart(lowerTerm);
                    if ((lowerTermHasDecimalPart == false && includeLower == false) ||
                            (lowerTermHasDecimalPart && signum(lowerTerm) > 0)) {
                        if (l == Integer.MAX_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        ++l;
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, true);
                    boolean upperTermHasDecimalPart = hasDecimalPart(upperTerm);
                    if ((upperTermHasDecimalPart == false && includeUpper == false) ||
                            (upperTermHasDecimalPart && signum(upperTerm) < 0)) {
                        if (u == Integer.MIN_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        --u;
                    }
                }
                Query query = IntPoint.newRangeQuery(field, l, u);
                if (hasDocValues) {
                    Query dvQuery = SortedNumericDocValuesField.newRangeQuery(field, l, u);
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                }
                return query;
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new IntPoint(name, value.intValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name, value.intValue()));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.intValue()));
                }
                return fields;
            }

            @Override
            FieldStats.Long stats(IndexReader reader, String fieldName,
                                  boolean isSearchable, boolean isAggregatable) throws IOException {
                FieldInfo fi = org.apache.lucene.index.MultiFields.getMergedFieldInfos(reader).fieldInfo(fieldName);
                if (fi == null) {
                    return null;
                }
                long size = PointValues.size(reader, fieldName);
                if (size == 0) {
                    return new FieldStats.Long(reader.maxDoc(), 0, -1, -1, isSearchable, isAggregatable);
                }
                int docCount = PointValues.getDocCount(reader, fieldName);
                byte[] min = PointValues.getMinPackedValue(reader, fieldName);
                byte[] max = PointValues.getMaxPackedValue(reader, fieldName);
                return new FieldStats.Long(reader.maxDoc(),docCount, -1L, size,
                    isSearchable, isAggregatable,
                    IntPoint.decodeDimension(min, 0), IntPoint.decodeDimension(max, 0));
            }
        },
        LONG("long", NumericType.LONG) {
            @Override
            Long parse(Object value, boolean coerce) {
                if (value instanceof Number) {
                    double doubleValue = ((Number) value).doubleValue();
                    if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                        throw new IllegalArgumentException("Value [" + value + "] is out of range for a long");
                    }
                    if (!coerce && doubleValue % 1 != 0) {
                        throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                    }
                    return ((Number) value).longValue();
                }
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                return Long.parseLong(value.toString());
            }

            @Override
            Long parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.longValue(coerce);
            }

            @Override
            Query termQuery(String field, Object value) {
                if (hasDecimalPart(value)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
                }
                long v = parse(value, true);
                return LongPoint.newExactQuery(field, v);
            }

            @Override
            Query termsQuery(String field, List<Object> values) {
                long[] v = new long[values.size()];
                int upTo = 0;

                for (int i = 0; i < values.size(); i++) {
                    Object value = values.get(i);
                    if (!hasDecimalPart(value)) {
                        v[upTo++] = parse(value, true);
                    }
                }

                if (upTo == 0) {
                    return Queries.newMatchNoDocsQuery("All values have a decimal part");
                }
                if (upTo != v.length) {
                    v = Arrays.copyOf(v, upTo);
                }
                return LongPoint.newSetQuery(field, v);
            }

            @Override
            Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                long l = Long.MIN_VALUE;
                long u = Long.MAX_VALUE;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, true);
                    // if the lower bound is decimal:
                    // - if the bound is positive then we increment it:
                    //      if lowerTerm=1.5 then the (inclusive) bound becomes 2
                    // - if the bound is negative then we leave it as is:
                    //      if lowerTerm=-1.5 then the (inclusive) bound becomes -1 due to the call to longValue
                    boolean lowerTermHasDecimalPart = hasDecimalPart(lowerTerm);
                    if ((lowerTermHasDecimalPart == false && includeLower == false) ||
                            (lowerTermHasDecimalPart && signum(lowerTerm) > 0)) {
                        if (l == Long.MAX_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        ++l;
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, true);
                    boolean upperTermHasDecimalPart = hasDecimalPart(upperTerm);
                    if ((upperTermHasDecimalPart == false && includeUpper == false) ||
                            (upperTermHasDecimalPart && signum(upperTerm) < 0)) {
                        if (u == Long.MIN_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        --u;
                    }
                }
                Query query = LongPoint.newRangeQuery(field, l, u);
                if (hasDocValues) {
                    Query dvQuery = SortedNumericDocValuesField.newRangeQuery(field, l, u);
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                }
                return query;
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new LongPoint(name, value.longValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name, value.longValue()));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.longValue()));
                }
                return fields;
            }

            @Override
            FieldStats.Long stats(IndexReader reader, String fieldName,
                                  boolean isSearchable, boolean isAggregatable) throws IOException {
                FieldInfo fi = org.apache.lucene.index.MultiFields.getMergedFieldInfos(reader).fieldInfo(fieldName);
                if (fi == null) {
                    return null;
                }
                long size = PointValues.size(reader, fieldName);
                if (size == 0) {
                    return new FieldStats.Long(reader.maxDoc(), 0, -1, -1, isSearchable, isAggregatable);
                }
                int docCount = PointValues.getDocCount(reader, fieldName);
                byte[] min = PointValues.getMinPackedValue(reader, fieldName);
                byte[] max = PointValues.getMaxPackedValue(reader, fieldName);
                return new FieldStats.Long(reader.maxDoc(),docCount, -1L, size,
                    isSearchable, isAggregatable,
                    LongPoint.decodeDimension(min, 0), LongPoint.decodeDimension(max, 0));
            }
        };

        private final String name;
        private final NumericType numericType;

        NumberType(String name, NumericType numericType) {
            this.name = name;
            this.numericType = numericType;
        }

        /** Get the associated type name. */
        public final String typeName() {
            return name;
        }
        /** Get the associated numeric type */
        final NumericType numericType() {
            return numericType;
        }
        abstract Query termQuery(String field, Object value);
        abstract Query termsQuery(String field, List<Object> values);
        abstract Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                                  boolean includeLower, boolean includeUpper,
                                  boolean hasDocValues);
        abstract Number parse(XContentParser parser, boolean coerce) throws IOException;
        abstract Number parse(Object value, boolean coerce);
        public abstract List<Field> createFields(String name, Number value, boolean indexed,
                                                 boolean docValued, boolean stored);
        abstract FieldStats<? extends Number> stats(IndexReader reader, String fieldName,
                                                    boolean isSearchable, boolean isAggregatable) throws IOException;
        Number valueForSearch(Number value) {
            return value;
        }

        /**
         * Returns true if the object is a number and has a decimal part
         */
        boolean hasDecimalPart(Object number) {
            if (number instanceof Number) {
                double doubleValue = ((Number) number).doubleValue();
                return doubleValue % 1 != 0;
            }
            if (number instanceof BytesRef) {
                number = ((BytesRef) number).utf8ToString();
            }
            if (number instanceof String) {
                return Double.parseDouble((String) number) % 1 != 0;
            }
            return false;
        }

        /**
         * Returns -1, 0, or 1 if the value is lower than, equal to, or greater than 0
         */
        double signum(Object value) {
            if (value instanceof Number) {
                double doubleValue = ((Number) value).doubleValue();
                return Math.signum(doubleValue);
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return Math.signum(Double.parseDouble(value.toString()));
        }

    }

    public static final class NumberFieldType extends MappedFieldType {

        NumberType type;

        public NumberFieldType(NumberType type) {
            super();
            this.type = Objects.requireNonNull(type);
            setTokenized(false);
            setHasDocValues(true);
            setOmitNorms(true);
        }

        NumberFieldType(NumberFieldType other) {
            super(other);
            this.type = other.type;
        }

        @Override
        public MappedFieldType clone() {
            return new NumberFieldType(this);
        }

        @Override
        public String typeName() {
            return type.name;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            failIfNotIndexed();
            Query query = type.termQuery(name(), value);
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query termsQuery(List values, QueryShardContext context) {
            failIfNotIndexed();
            Query query = type.termsQuery(name(), values);
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            failIfNotIndexed();
            Query query = type.rangeQuery(name(), lowerTerm, upperTerm, includeLower, includeUpper, hasDocValues());
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public FieldStats stats(IndexReader reader) throws IOException {
            return type.stats(reader, name(), isSearchable(), isAggregatable());
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder().numericType(type.numericType());
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return type.valueForSearch((Number) value);
        }

        @Override
        public DocValueFormat docValueFormat(String format, DateTimeZone timeZone) {
            if (timeZone != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName()
                    + "] does not support custom time zones");
            }
            if (format == null) {
                return DocValueFormat.RAW;
            } else {
                return new DocValueFormat.Decimal(format);
            }
        }
    }

    private Boolean includeInAll;

    private Explicit<Boolean> ignoreMalformed;

    private Explicit<Boolean> coerce;

    private NumberFieldMapper(
            String simpleName,
            MappedFieldType fieldType,
            MappedFieldType defaultFieldType,
            Explicit<Boolean> ignoreMalformed,
            Explicit<Boolean> coerce,
            Boolean includeInAll,
            Settings indexSettings,
            MultiFields multiFields,
            CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.coerce = coerce;
        this.includeInAll = includeInAll;
    }

    @Override
    public NumberFieldType fieldType() {
        return (NumberFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType.typeName();
    }

    @Override
    protected NumberFieldMapper clone() {
        return (NumberFieldMapper) super.clone();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        final boolean includeInAll = context.includeInAll(this.includeInAll, this);

        XContentParser parser = context.parser();
        Object value;
        Number numericValue = null;
        if (context.externalValueSet()) {
            value = context.externalValue();
        } else if (parser.currentToken() == Token.VALUE_NULL) {
            value = null;
        } else if (coerce.value()
                && parser.currentToken() == Token.VALUE_STRING
                && parser.textLength() == 0) {
            value = null;
        } else {
            try {
                numericValue = fieldType().type.parse(parser, coerce.value());
            } catch (IllegalArgumentException e) {
                if (ignoreMalformed.value()) {
                    return;
                } else {
                    throw e;
                }
            }
            if (includeInAll) {
                value = parser.textOrNull(); // preserve formatting
            } else {
                value = numericValue;
            }
        }

        if (value == null) {
            value = fieldType().nullValue();
        }

        if (value == null) {
            return;
        }

        if (numericValue == null) {
            numericValue = fieldType().type.parse(value, coerce.value());
        }

        if (includeInAll) {
            context.allEntries().addText(fieldType().name(), value.toString(), fieldType().boost());
        }

        boolean indexed = fieldType().indexOptions() != IndexOptions.NONE;
        boolean docValued = fieldType().hasDocValues();
        boolean stored = fieldType().stored();
        fields.addAll(fieldType().type.createFields(fieldType().name(), numericValue, indexed, docValued, stored));
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        NumberFieldMapper other = (NumberFieldMapper) mergeWith;
        this.includeInAll = other.includeInAll;
        if (other.ignoreMalformed.explicit()) {
            this.ignoreMalformed = other.ignoreMalformed;
        }
        if (other.coerce.explicit()) {
            this.coerce = other.coerce;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field("ignore_malformed", ignoreMalformed.value());
        }
        if (includeDefaults || coerce.explicit()) {
            builder.field("coerce", coerce.value());
        }

        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field("null_value", fieldType().nullValue());
        }

        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", false);
        }
    }
}
