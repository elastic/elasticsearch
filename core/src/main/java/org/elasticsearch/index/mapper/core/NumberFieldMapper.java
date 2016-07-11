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

package org.elasticsearch.index.mapper.core;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.HalfFloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.XPointValues;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.LegacyNumberFieldMapper.Defaults;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A {@link FieldMapper} for numeric types: byte, short, int, long, float and double. */
public class NumberFieldMapper extends FieldMapper implements AllFieldMapper.IncludeInAll {

    // this is private since it has a different default
    private static final Setting<Boolean> COERCE_SETTING =
            Setting.boolSetting("index.mapping.coerce", true, Property.IndexScope);

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
            NumberFieldMapper fieldMapper =
                new NumberFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context),
                    coerce(context), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            return (NumberFieldMapper) fieldMapper.includeInAll(includeInAll);
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
            if (parserContext.indexVersionCreated().before(Version.V_5_0_0_alpha2)) {
                switch (type) {
                case BYTE:
                    return new LegacyByteFieldMapper.TypeParser().parse(name, node, parserContext);
                case SHORT:
                    return new LegacyShortFieldMapper.TypeParser().parse(name, node, parserContext);
                case INTEGER:
                    return new LegacyIntegerFieldMapper.TypeParser().parse(name, node, parserContext);
                case LONG:
                    return new LegacyLongFieldMapper.TypeParser().parse(name, node, parserContext);
                case FLOAT:
                    return new LegacyFloatFieldMapper.TypeParser().parse(name, node, parserContext);
                case DOUBLE:
                    return new LegacyDoubleFieldMapper.TypeParser().parse(name, node, parserContext);
                default:
                    throw new AssertionError();
                }
            }
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
                    builder.nullValue(type.parse(propNode));
                    iterator.remove();
                } else if (propName.equals("ignore_malformed")) {
                    builder.ignoreMalformed(TypeParsers.nodeBooleanValue("ignore_malformed", propNode, parserContext));
                    iterator.remove();
                } else if (propName.equals("coerce")) {
                    builder.coerce(TypeParsers.nodeBooleanValue("coerce", propNode, parserContext));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public enum NumberType {
        HALF_FLOAT("half_float", NumericType.HALF_FLOAT) {
            @Override
            Float parse(Object value) {
                return (Float) FLOAT.parse(value);
            }

            @Override
            Float parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.floatValue(coerce);
            }

            @Override
            Query termQuery(String field, Object value) {
                float v = parse(value);
                return HalfFloatPoint.newExactQuery(field, v);
            }

            @Override
            Query termsQuery(String field, List<Object> values) {
                float[] v = new float[values.size()];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i));
                }
                return HalfFloatPoint.newSetQuery(field, v);
            }

            @Override
            Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper) {
                float l = Float.NEGATIVE_INFINITY;
                float u = Float.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parse(lowerTerm);
                    if (includeLower) {
                        l = Math.nextDown(l);
                    }
                    l = HalfFloatPoint.nextUp(l);
                }
                if (upperTerm != null) {
                    u = parse(upperTerm);
                    if (includeUpper) {
                        u = Math.nextUp(u);
                    }
                    u = HalfFloatPoint.nextDown(u);
                }
                return HalfFloatPoint.newRangeQuery(field, l, u);
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
                long size = XPointValues.size(reader, fieldName);
                if (size == 0) {
                    return null;
                }
                int docCount = XPointValues.getDocCount(reader, fieldName);
                byte[] min = XPointValues.getMinPackedValue(reader, fieldName);
                byte[] max = XPointValues.getMaxPackedValue(reader, fieldName);
                return new FieldStats.Double(reader.maxDoc(),docCount, -1L, size,
                    isSearchable, isAggregatable,
                    HalfFloatPoint.decodeDimension(min, 0), HalfFloatPoint.decodeDimension(max, 0));
            }
        },
        FLOAT("float", NumericType.FLOAT) {
            @Override
            Float parse(Object value) {
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
                float v = parse(value);
                return FloatPoint.newExactQuery(field, v);
            }

            @Override
            Query termsQuery(String field, List<Object> values) {
                float[] v = new float[values.size()];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i));
                }
                return FloatPoint.newSetQuery(field, v);
            }

            @Override
            Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper) {
                float l = Float.NEGATIVE_INFINITY;
                float u = Float.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parse(lowerTerm);
                    if (includeLower == false) {
                        l = Math.nextUp(l);
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm);
                    if (includeUpper == false) {
                        u = Math.nextDown(u);
                    }
                }
                return FloatPoint.newRangeQuery(field, l, u);
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
                long size = XPointValues.size(reader, fieldName);
                if (size == 0) {
                    return null;
                }
                int docCount = XPointValues.getDocCount(reader, fieldName);
                byte[] min = XPointValues.getMinPackedValue(reader, fieldName);
                byte[] max = XPointValues.getMaxPackedValue(reader, fieldName);
                return new FieldStats.Double(reader.maxDoc(),docCount, -1L, size,
                    isSearchable, isAggregatable,
                    FloatPoint.decodeDimension(min, 0), FloatPoint.decodeDimension(max, 0));
            }
        },
        DOUBLE("double", NumericType.DOUBLE) {
            @Override
            Double parse(Object value) {
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
                double v = parse(value);
                return DoublePoint.newExactQuery(field, v);
            }

            @Override
            Query termsQuery(String field, List<Object> values) {
                double[] v = new double[values.size()];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i));
                }
                return DoublePoint.newSetQuery(field, v);
            }

            @Override
            Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper) {
                double l = Double.NEGATIVE_INFINITY;
                double u = Double.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parse(lowerTerm);
                    if (includeLower == false) {
                        l = Math.nextUp(l);
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm);
                    if (includeUpper == false) {
                        u = Math.nextDown(u);
                    }
                }
                return DoublePoint.newRangeQuery(field, l, u);
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
                long size = XPointValues.size(reader, fieldName);
                if (size == 0) {
                    return null;
                }
                int docCount = XPointValues.getDocCount(reader, fieldName);
                byte[] min = XPointValues.getMinPackedValue(reader, fieldName);
                byte[] max = XPointValues.getMaxPackedValue(reader, fieldName);
                return new FieldStats.Double(reader.maxDoc(),docCount, -1L, size,
                    isSearchable, isAggregatable,
                    DoublePoint.decodeDimension(min, 0), DoublePoint.decodeDimension(max, 0));
            }
        },
        BYTE("byte", NumericType.BYTE) {
            @Override
            Byte parse(Object value) {
                if (value instanceof Number) {
                    double doubleValue = ((Number) value).doubleValue();
                    if (doubleValue < Byte.MIN_VALUE || doubleValue > Byte.MAX_VALUE) {
                        throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
                    }
                    if (doubleValue % 1 != 0) {
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
                             boolean includeLower, boolean includeUpper) {
                return INTEGER.rangeQuery(field, lowerTerm, upperTerm, includeLower, includeUpper);
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
            Short parse(Object value) {
                if (value instanceof Number) {
                    double doubleValue = ((Number) value).doubleValue();
                    if (doubleValue < Short.MIN_VALUE || doubleValue > Short.MAX_VALUE) {
                        throw new IllegalArgumentException("Value [" + value + "] is out of range for a short");
                    }
                    if (doubleValue % 1 != 0) {
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
                             boolean includeLower, boolean includeUpper) {
                return INTEGER.rangeQuery(field, lowerTerm, upperTerm, includeLower, includeUpper);
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
            Integer parse(Object value) {
                if (value instanceof Number) {
                    double doubleValue = ((Number) value).doubleValue();
                    if (doubleValue < Integer.MIN_VALUE || doubleValue > Integer.MAX_VALUE) {
                        throw new IllegalArgumentException("Value [" + value + "] is out of range for an integer");
                    }
                    if (doubleValue % 1 != 0) {
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
                int v = parse(value);
                return IntPoint.newExactQuery(field, v);
            }

            @Override
            Query termsQuery(String field, List<Object> values) {
                int[] v = new int[values.size()];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i));
                }
                return IntPoint.newSetQuery(field, v);
            }

            @Override
            Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper) {
                int l = Integer.MIN_VALUE;
                int u = Integer.MAX_VALUE;
                if (lowerTerm != null) {
                    l = parse(lowerTerm);
                    if (includeLower == false) {
                        if (l == Integer.MAX_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        ++l;
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm);
                    if (includeUpper == false) {
                        if (u == Integer.MIN_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        --u;
                    }
                }
                return IntPoint.newRangeQuery(field, l, u);
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
                long size = XPointValues.size(reader, fieldName);
                if (size == 0) {
                    return null;
                }
                int docCount = XPointValues.getDocCount(reader, fieldName);
                byte[] min = XPointValues.getMinPackedValue(reader, fieldName);
                byte[] max = XPointValues.getMaxPackedValue(reader, fieldName);
                return new FieldStats.Long(reader.maxDoc(),docCount, -1L, size,
                    isSearchable, isAggregatable,
                    IntPoint.decodeDimension(min, 0), IntPoint.decodeDimension(max, 0));
            }
        },
        LONG("long", NumericType.LONG) {
            @Override
            Long parse(Object value) {
                if (value instanceof Number) {
                    double doubleValue = ((Number) value).doubleValue();
                    if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                        throw new IllegalArgumentException("Value [" + value + "] is out of range for a long");
                    }
                    if (doubleValue % 1 != 0) {
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
                long v = parse(value);
                return LongPoint.newExactQuery(field, v);
            }

            @Override
            Query termsQuery(String field, List<Object> values) {
                long[] v = new long[values.size()];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i));
                }
                return LongPoint.newSetQuery(field, v);
            }

            @Override
            Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper) {
                long l = Long.MIN_VALUE;
                long u = Long.MAX_VALUE;
                if (lowerTerm != null) {
                    l = parse(lowerTerm);
                    if (includeLower == false) {
                        if (l == Long.MAX_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        ++l;
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm);
                    if (includeUpper == false) {
                        if (u == Long.MIN_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        --u;
                    }
                }
                return LongPoint.newRangeQuery(field, l, u);
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
                long size = XPointValues.size(reader, fieldName);
                if (size == 0) {
                    return null;
                }
                int docCount = XPointValues.getDocCount(reader, fieldName);
                byte[] min = XPointValues.getMinPackedValue(reader, fieldName);
                byte[] max = XPointValues.getMaxPackedValue(reader, fieldName);
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
        /** Get the associated numerit type */
        final NumericType numericType() {
            return numericType;
        }
        abstract Query termQuery(String field, Object value);
        abstract Query termsQuery(String field, List<Object> values);
        abstract Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                                  boolean includeLower, boolean includeUpper);
        abstract Number parse(XContentParser parser, boolean coerce) throws IOException;
        abstract Number parse(Object value);
        public abstract List<Field> createFields(String name, Number value, boolean indexed,
                                                 boolean docValued, boolean stored);
        abstract FieldStats<? extends Number> stats(IndexReader reader, String fieldName,
                                                    boolean isSearchable, boolean isAggregatable) throws IOException;
        Number valueForSearch(Number value) {
            return value;
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
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper) {
            failIfNotIndexed();
            Query query = type.rangeQuery(name(), lowerTerm, upperTerm, includeLower, includeUpper);
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
        public Object valueForSearch(Object value) {
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
            Settings indexSettings,
            MultiFields multiFields,
            CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.coerce = coerce;
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
    public Mapper includeInAll(Boolean includeInAll) {
        if (includeInAll != null) {
            NumberFieldMapper clone = clone();
            clone.includeInAll = includeInAll;
            return clone;
        } else {
            return this;
        }
    }

    @Override
    public Mapper includeInAllIfNotSet(Boolean includeInAll) {
        if (includeInAll != null && this.includeInAll == null) {
            NumberFieldMapper clone = clone();
            clone.includeInAll = includeInAll;
            return clone;
        } else {
            return this;
        }
    }

    @Override
    public Mapper unsetIncludeInAll() {
        if (includeInAll != null) {
            NumberFieldMapper clone = clone();
            clone.includeInAll = null;
            return clone;
        } else {
            return this;
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
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
            value = parser.textOrNull();
            if (value != null) {
                try {
                    numericValue = fieldType().type.parse(parser, coerce.value());
                } catch (IllegalArgumentException e) {
                    if (ignoreMalformed.value()) {
                        return;
                    } else {
                        throw e;
                    }
                }
            }
        }

        if (value == null) {
            value = fieldType().nullValue();
        }

        if (value == null) {
            return;
        }

        if (numericValue == null) {
            numericValue = fieldType().type.parse(value);
        }

        if (context.includeInAll(includeInAll, this)) {
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
