/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.exc.InputCoercionException;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.HalfFloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/** A {@link FieldMapper} for numeric types: byte, short, int, long, float and double. */
public class NumberFieldMapper extends FieldMapper {

    public static final Setting<Boolean> COERCE_SETTING =
            Setting.boolSetting("index.mapping.coerce", true, Property.IndexScope);

    private static NumberFieldMapper toType(FieldMapper in) {
        return (NumberFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, true);
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);

        private final Parameter<Explicit<Boolean>> ignoreMalformed;
        private final Parameter<Explicit<Boolean>> coerce;

        private final Parameter<Number> nullValue;

        private final Parameter<Script> script = Parameter.scriptParam(m -> toType(m).builder.script.get());
        private final Parameter<String> onScriptError = Parameter.onScriptErrorParam(m -> toType(m).onScriptError, script);
        private final Parameter<Boolean> dimension;


        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final ScriptCompiler scriptCompiler;
        private final NumberType type;

        public Builder(String name, NumberType type, ScriptCompiler compiler, Settings settings) {
            this(name, type, compiler, IGNORE_MALFORMED_SETTING.get(settings), COERCE_SETTING.get(settings));
        }

        public static Builder docValuesOnly(String name, NumberType type) {
            Builder builder = new Builder(name, type, ScriptCompiler.NONE, false, false);
            builder.indexed.setValue(false);
            builder.dimension.setValue(false);
            return builder;
        }

        public Builder(String name, NumberType type, ScriptCompiler compiler, boolean ignoreMalformedByDefault, boolean coerceByDefault) {
            super(name);
            this.type = type;
            this.scriptCompiler = Objects.requireNonNull(compiler);

            this.ignoreMalformed
                = Parameter.explicitBoolParam("ignore_malformed", true, m -> toType(m).ignoreMalformed, ignoreMalformedByDefault);
            this.coerce
                = Parameter.explicitBoolParam("coerce", true, m -> toType(m).coerce, coerceByDefault);
            this.nullValue = new Parameter<>("null_value", false, () -> null,
                (n, c, o) -> o == null ? null : type.parse(o, false), m -> toType(m).nullValue).acceptsNull();

            this.dimension = Parameter.boolParam("dimension", false, m -> toType(m).dimension, false)
                .setValidator(v -> {
                    if (v && EnumSet.of(NumberType.INTEGER, NumberType.LONG, NumberType.BYTE, NumberType.SHORT).contains(type) == false) {
                        throw new IllegalArgumentException("Parameter [dimension] cannot be set to numeric type [" + type.name + "]");
                    }
                    if (v && (indexed.getValue() == false || hasDocValues.getValue() == false)) {
                        throw new IllegalArgumentException(
                            "Field [dimension] requires that [" + indexed.name + "] and [" + hasDocValues.name + "] are true"
                        );
                    }
                });

            this.script.precludesParameters(ignoreMalformed, coerce, nullValue);
            addScriptValidation(script, indexed, hasDocValues);
        }

        Builder nullValue(Number number) {
            this.nullValue.setValue(number);
            return this;
        }

        public Builder docValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
            return this;
        }

        private FieldValues<Number> scriptValues() {
            if (this.script.get() == null) {
                return null;
            }
            return type.compile(name, script.get(), scriptCompiler);
        }

        public Builder dimension(boolean dimension) {
            this.dimension.setValue(dimension);
            return this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(indexed, hasDocValues, stored, ignoreMalformed, coerce, nullValue, script, onScriptError, meta, dimension);
        }

        @Override
        public NumberFieldMapper build(ContentPath contentPath) {
            MappedFieldType ft = new NumberFieldType(buildFullName(contentPath), this);
            return new NumberFieldMapper(name, ft, multiFieldsBuilder.build(this, contentPath), copyTo.build(), this);
        }
    }

    public enum NumberType {
        HALF_FLOAT("half_float", NumericType.HALF_FLOAT) {
            @Override
            public Float parse(Object value, boolean coerce) {
                final float result = parseToFloat(value);
                // Reduce the precision to what we actually index
                return HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(result));
            }

            /**
             * Parse a query parameter or {@code _source} value to a float,
             * keeping float precision. Used by queries which need more
             * precise control over their rounding behavior that
             * {@link #parse(Object, boolean)} provides.
             */
            private float parseToFloat(Object value) {
                final float result;

                if (value instanceof Number) {
                    result = ((Number) value).floatValue();
                } else {
                    if (value instanceof BytesRef) {
                        value = ((BytesRef) value).utf8ToString();
                    }
                    result = Float.parseFloat(value.toString());
                }
                validateParsed(result);
                return result;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return HalfFloatPoint.decodeDimension(value, 0);
            }

            @Override
            public Float parse(XContentParser parser, boolean coerce) throws IOException {
                float parsed = parser.floatValue(coerce);
                validateParsed(parsed);
                return parsed;
            }

            @Override
            public Query termQuery(String field, Object value) {
                float v = parseToFloat(value);
                return HalfFloatPoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                float[] v = new float[values.size()];
                int pos = 0;
                for (Object value: values) {
                    v[pos++] = parseToFloat(value);
                }
                return HalfFloatPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                                    boolean includeLower, boolean includeUpper,
                                    boolean hasDocValues, SearchExecutionContext context) {
                float l = Float.NEGATIVE_INFINITY;
                float u = Float.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parseToFloat(lowerTerm);
                    if (includeLower) {
                        l = HalfFloatPoint.nextDown(l);
                    }
                    l = HalfFloatPoint.nextUp(l);
                }
                if (upperTerm != null) {
                    u = parseToFloat(upperTerm);
                    if (includeUpper) {
                        u = HalfFloatPoint.nextUp(u);
                    }
                    u = HalfFloatPoint.nextDown(u);
                }
                Query query = HalfFloatPoint.newRangeQuery(field, l, u);
                if (hasDocValues) {
                    Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field,
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

            private void validateParsed(float value) {
                if (Float.isFinite(HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(value))) == false) {
                    throw new IllegalArgumentException("[half_float] supports only finite values, but got [" + value + "]");
                }
            }
        },
        FLOAT("float", NumericType.FLOAT) {
            @Override
            public Float parse(Object value, boolean coerce) {
                final float result;

                if (value instanceof Number) {
                    result = ((Number) value).floatValue();
                } else {
                    if (value instanceof BytesRef) {
                        value = ((BytesRef) value).utf8ToString();
                    }
                    result = Float.parseFloat(value.toString());
                }
                validateParsed(result);
                return result;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return FloatPoint.decodeDimension(value, 0);
            }

            @Override
            public Float parse(XContentParser parser, boolean coerce) throws IOException {
                float parsed = parser.floatValue(coerce);
                validateParsed(parsed);
                return parsed;
            }

            @Override
            public Query termQuery(String field, Object value) {
                float v = parse(value, false);
                return FloatPoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                float[] v = new float[values.size()];
                int pos = 0;
                for (Object value: values) {
                    v[pos++] = parse(value, false);
                }
                return FloatPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                                    boolean includeLower, boolean includeUpper,
                                    boolean hasDocValues, SearchExecutionContext context) {
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
                    Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field,
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

            private void validateParsed(float value) {
                if (Float.isFinite(value) == false) {
                    throw new IllegalArgumentException("[float] supports only finite values, but got [" + value + "]");
                }
            }
        },
        DOUBLE("double", NumericType.DOUBLE) {
            @Override
            public Double parse(Object value, boolean coerce) {
                double parsed = objectToDouble(value);
                validateParsed(parsed);
                return parsed;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return DoublePoint.decodeDimension(value, 0);
            }

            @Override
            public Double parse(XContentParser parser, boolean coerce) throws IOException {
                double parsed = parser.doubleValue(coerce);
                validateParsed(parsed);
                return parsed;
            }

            @Override
            public FieldValues<Number> compile(String fieldName, Script script, ScriptCompiler compiler) {
                DoubleFieldScript.Factory scriptFactory = compiler.compile(script, DoubleFieldScript.CONTEXT);
                return (lookup, ctx, doc, consumer) -> scriptFactory
                    .newFactory(fieldName, script.getParams(), lookup)
                    .newInstance(ctx)
                    .runForDoc(doc, consumer::accept);
            }

            @Override
            public Query termQuery(String field, Object value) {
                double v = parse(value, false);
                return DoublePoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                double[] v = values.stream().mapToDouble(value -> parse(value, false)).toArray();
                return DoublePoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                                    boolean includeLower, boolean includeUpper,
                                    boolean hasDocValues, SearchExecutionContext context) {
                return doubleRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, (l, u) -> {
                    Query query = DoublePoint.newRangeQuery(field, l, u);
                    if (hasDocValues) {
                        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field,
                                NumericUtils.doubleToSortableLong(l),
                                NumericUtils.doubleToSortableLong(u));
                        query = new IndexOrDocValuesQuery(query, dvQuery);
                    }
                    return query;
                });
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

            private void validateParsed(double value) {
                if (Double.isFinite(value) == false) {
                    throw new IllegalArgumentException("[double] supports only finite values, but got [" + value + "]");
                }
            }
        },
        BYTE("byte", NumericType.BYTE) {
            @Override
            public Byte parse(Object value, boolean coerce) {
                double doubleValue = objectToDouble(value);

                if (doubleValue < Byte.MIN_VALUE || doubleValue > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
                }
                if (coerce == false && doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }

                if (value instanceof Number) {
                    return ((Number) value).byteValue();
                }

                return (byte) doubleValue;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return INTEGER.parsePoint(value).byteValue();
            }

            @Override
            public Short parse(XContentParser parser, boolean coerce) throws IOException {
                int value = parser.intValue(coerce);
                if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
                }
                return (short) value;
            }

            @Override
            public Query termQuery(String field, Object value) {
                return INTEGER.termQuery(field, value);
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                return INTEGER.termsQuery(field, values);
            }

            @Override
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                                    boolean includeLower, boolean includeUpper,
                                    boolean hasDocValues, SearchExecutionContext context) {
                return INTEGER.rangeQuery(field, lowerTerm, upperTerm, includeLower, includeUpper, hasDocValues, context);
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                return INTEGER.createFields(name, value, indexed, docValued, stored);
            }

            @Override
            Number valueForSearch(Number value) {
                return value.byteValue();
            }
        },
        SHORT("short", NumericType.SHORT) {
            @Override
            public Short parse(Object value, boolean coerce) {
                double doubleValue = objectToDouble(value);

                if (doubleValue < Short.MIN_VALUE || doubleValue > Short.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a short");
                }
                if (coerce == false && doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }

                if (value instanceof Number) {
                    return ((Number) value).shortValue();
                }

                return (short) doubleValue;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return INTEGER.parsePoint(value).shortValue();
            }

            @Override
            public Short parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.shortValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value) {
                return INTEGER.termQuery(field, value);
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                return INTEGER.termsQuery(field, values);
            }

            @Override
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                                    boolean includeLower, boolean includeUpper,
                                    boolean hasDocValues, SearchExecutionContext context) {
                return INTEGER.rangeQuery(field, lowerTerm, upperTerm, includeLower, includeUpper, hasDocValues, context);
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                return INTEGER.createFields(name, value, indexed, docValued, stored);
            }

            @Override
            Number valueForSearch(Number value) {
                return value.shortValue();
            }
        },
        INTEGER("integer", NumericType.INT) {
            @Override
            public Integer parse(Object value, boolean coerce) {
                double doubleValue = objectToDouble(value);

                if (doubleValue < Integer.MIN_VALUE || doubleValue > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for an integer");
                }
                if (coerce == false && doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }

                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }

                return (int) doubleValue;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return IntPoint.decodeDimension(value, 0);
            }

            @Override
            public Integer parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.intValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value) {
                if (hasDecimalPart(value)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
                }
                int v = parse(value, true);
                return IntPoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                int[] v = new int[values.size()];
                int upTo = 0;

                for (Object value : values) {
                    if (hasDecimalPart(value) == false) {
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
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                                    boolean includeLower, boolean includeUpper,
                                    boolean hasDocValues, SearchExecutionContext context) {
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
                    Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field, l, u);
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                    if (context.indexSortedOnField(field)) {
                        query = new IndexSortSortedNumericDocValuesRangeQuery(field, l, u, query);
                    }
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
        },
        LONG("long", NumericType.LONG) {
            @Override
            public Long parse(Object value, boolean coerce) {
                return objectToLong(value, coerce);
            }

            @Override
            public Number parsePoint(byte[] value) {
                return LongPoint.decodeDimension(value, 0);
            }

            @Override
            public Long parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.longValue(coerce);
            }

            @Override
            public FieldValues<Number> compile(String fieldName, Script script, ScriptCompiler compiler) {
                final LongFieldScript.Factory scriptFactory = compiler.compile(script, LongFieldScript.CONTEXT);
                return (lookup, ctx, doc, consumer) -> scriptFactory
                    .newFactory(fieldName, script.getParams(), lookup)
                    .newInstance(ctx)
                    .runForDoc(doc, consumer::accept);
            }

            @Override
            public Query termQuery(String field, Object value) {
                if (hasDecimalPart(value)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
                }
                long v = parse(value, true);
                return LongPoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, Collection<?> values) {
                long[] v = new long[values.size()];
                int upTo = 0;

                for (Object value : values) {
                    if (hasDecimalPart(value) == false) {
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
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                                    boolean includeLower, boolean includeUpper,
                                    boolean hasDocValues, SearchExecutionContext context) {
                return longRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, (l, u) -> {
                    Query query = LongPoint.newRangeQuery(field, l, u);
                    if (hasDocValues) {
                        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field, l, u);
                        query = new IndexOrDocValuesQuery(query, dvQuery);
                        if (context.indexSortedOnField(field)) {
                            query = new IndexSortSortedNumericDocValuesRangeQuery(field, l, u, query);
                        }
                    }
                    return query;
                });
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
        };

        private final String name;
        private final NumericType numericType;
        private final TypeParser parser;

        NumberType(String name, NumericType numericType) {
            this.name = name;
            this.numericType = numericType;
            this.parser = new TypeParser((n, c) -> new Builder(n, this, c.scriptCompiler(), c.getSettings()));
        }

        /** Get the associated type name. */
        public final String typeName() {
            return name;
        }
        /** Get the associated numeric type */
        public final NumericType numericType() {
            return numericType;
        }
        public final TypeParser parser() {
            return parser;
        }
        public abstract Query termQuery(String field, Object value);
        public abstract Query termsQuery(String field, Collection<?> values);
        public abstract Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                                         boolean includeLower, boolean includeUpper,
                                         boolean hasDocValues, SearchExecutionContext context);
        public abstract Number parse(XContentParser parser, boolean coerce) throws IOException;
        public abstract Number parse(Object value, boolean coerce);
        public abstract Number parsePoint(byte[] value);
        public abstract List<Field> createFields(String name, Number value, boolean indexed,
                                                 boolean docValued, boolean stored);

        public FieldValues<Number> compile(String fieldName, Script script, ScriptCompiler compiler) {
            // only implemented for long and double fields
            throw new IllegalArgumentException("Unknown parameter [script] for mapper [" + fieldName + "]");
        }

        Number valueForSearch(Number value) {
            return value;
        }

        /**
         * Returns true if the object is a number and has a decimal part
         */
        public static boolean hasDecimalPart(Object number) {
            if (number instanceof Byte
                || number instanceof Short
                || number instanceof Integer
                || number instanceof Long) {
                return false;
            }
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
        static double signum(Object value) {
            if (value instanceof Number) {
                double doubleValue = ((Number) value).doubleValue();
                return Math.signum(doubleValue);
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return Math.signum(Double.parseDouble(value.toString()));
        }

        /**
         * Converts an Object to a double by checking it against known types first
         */
        public static double objectToDouble(Object value) {
            double doubleValue;

            if (value instanceof Number) {
                doubleValue = ((Number) value).doubleValue();
            } else if (value instanceof BytesRef) {
                doubleValue = Double.parseDouble(((BytesRef) value).utf8ToString());
            } else {
                doubleValue = Double.parseDouble(value.toString());
            }

            return doubleValue;
        }

        /**
         * Converts an Object to a {@code long} by checking it against known
         * types and checking its range.
         */
        public static long objectToLong(Object value, boolean coerce) {
            if (value instanceof Long) {
                return (Long)value;
            }

            double doubleValue = objectToDouble(value);
            // this check does not guarantee that value is inside MIN_VALUE/MAX_VALUE because values up to 9223372036854776832 will
            // be equal to Long.MAX_VALUE after conversion to double. More checks ahead.
            if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                throw new IllegalArgumentException("Value [" + value + "] is out of range for a long");
            }
            if (coerce == false && doubleValue % 1 != 0) {
                throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
            }

            // longs need special handling so we don't lose precision while parsing
            String stringValue = (value instanceof BytesRef) ? ((BytesRef) value).utf8ToString() : value.toString();
            return Numbers.toLong(stringValue, coerce);
        }

        public static Query doubleRangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            BiFunction<Double, Double, Query> builder
        ) {
            double l = Double.NEGATIVE_INFINITY;
            double u = Double.POSITIVE_INFINITY;
            if (lowerTerm != null) {
                l = objectToDouble(lowerTerm);
                if (includeLower == false) {
                    l = DoublePoint.nextUp(l);
                }
            }
            if (upperTerm != null) {
                u = objectToDouble(upperTerm);
                if (includeUpper == false) {
                    u = DoublePoint.nextDown(u);
                }
            }
            return builder.apply(l, u);
        }

        /**
         * Processes query bounds into {@code long}s and delegates the
         * provided {@code builder} to build a range query.
         */
        public static Query longRangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            BiFunction<Long, Long, Query> builder
        ) {
            long l = Long.MIN_VALUE;
            long u = Long.MAX_VALUE;
            if (lowerTerm != null) {
                l = objectToLong(lowerTerm, true);
                // if the lower bound is decimal:
                // - if the bound is positive then we increment it:
                // if lowerTerm=1.5 then the (inclusive) bound becomes 2
                // - if the bound is negative then we leave it as is:
                // if lowerTerm=-1.5 then the (inclusive) bound becomes -1 due to the call to longValue
                boolean lowerTermHasDecimalPart = hasDecimalPart(lowerTerm);
                if ((lowerTermHasDecimalPart == false && includeLower == false) || (lowerTermHasDecimalPart && signum(lowerTerm) > 0)) {
                    if (l == Long.MAX_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    ++l;
                }
            }
            if (upperTerm != null) {
                u = objectToLong(upperTerm, true);
                boolean upperTermHasDecimalPart = hasDecimalPart(upperTerm);
                if ((upperTermHasDecimalPart == false && includeUpper == false) || (upperTermHasDecimalPart && signum(upperTerm) < 0)) {
                    if (u == Long.MIN_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    --u;
                }
            }
            return builder.apply(l, u);
        }
    }

    public static class NumberFieldType extends SimpleMappedFieldType {

        private final NumberType type;
        private final boolean coerce;
        private final Number nullValue;
        private final FieldValues<Number> scriptValues;
        private final boolean isDimension;

        public NumberFieldType(String name, NumberType type, boolean isSearchable, boolean isStored,
                               boolean hasDocValues, boolean coerce, Number nullValue, Map<String, String> meta,
                               FieldValues<Number> script, boolean isDimension) {
            super(name, isSearchable, isStored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
            this.type = Objects.requireNonNull(type);
            this.coerce = coerce;
            this.nullValue = nullValue;
            this.scriptValues = script;
            this.isDimension = isDimension;
        }

        NumberFieldType(String name, Builder builder) {
            this(name, builder.type, builder.indexed.getValue(), builder.stored.getValue(), builder.hasDocValues.getValue(),
                builder.coerce.getValue().value(), builder.nullValue.getValue(), builder.meta.getValue(),
                builder.scriptValues(), builder.dimension.getValue());
        }

        public NumberFieldType(String name, NumberType type) {
            this(name, type, true, false, true, true, null, Collections.emptyMap(), null, false);
        }

        @Override
        public String typeName() {
            return type.name;
        }

        public NumericType numericType() {
            return type.numericType();
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            failIfNotIndexed();
            return type.termQuery(name(), value);
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexed();
            return type.termsQuery(name(), values);
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper,
                                SearchExecutionContext context) {
            failIfNotIndexed();
            return type.rangeQuery(name(), lowerTerm, upperTerm, includeLower, includeUpper, hasDocValues(), context);
        }

        @Override
        public Function<byte[], Number> pointReaderIfPossible() {
            if (isSearchable()) {
                return this::parsePoint;
            }
            return null;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(name(), type.numericType());
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return type.valueForSearch((Number) value);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            if (this.scriptValues != null) {
                return FieldValues.valueFetcher(this.scriptValues, context);
            }
            return new SourceValueFetcher(name(), context, nullValue) {
                @Override
                protected Object parseSourceValue(Object value) {
                    if (value.equals("")) {
                        return nullValue;
                    }
                    return type.parse(value, coerce);
                }
            };
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
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

        public Number parsePoint(byte[] value) {
            return type.parsePoint(value);
        }

        @Override
        public CollapseType collapseType() {
            return CollapseType.NUMERIC;
        }

        /**
         * @return true if field has been marked as a dimension field
         */
        public boolean isDimension() {
            return isDimension;
        }
    }

    private final Builder builder;
    private final NumberType type;

    private final boolean indexed;
    private final boolean hasDocValues;
    private final boolean stored;
    private final Explicit<Boolean> ignoreMalformed;
    private final Explicit<Boolean> coerce;
    private final Number nullValue;
    private final FieldValues<Number> scriptValues;
    private final boolean ignoreMalformedByDefault;
    private final boolean coerceByDefault;
    private final boolean dimension;

    private NumberFieldMapper(
            String simpleName,
            MappedFieldType mappedFieldType,
            MultiFields multiFields,
            CopyTo copyTo,
            Builder builder) {
        super(simpleName, mappedFieldType, multiFields, copyTo, builder.script.get() != null, builder.onScriptError.getValue());
        this.type = builder.type;
        this.indexed = builder.indexed.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.stored = builder.stored.getValue();
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.coerce = builder.coerce.getValue();
        this.nullValue = builder.nullValue.getValue();
        this.ignoreMalformedByDefault = builder.ignoreMalformed.getDefaultValue().value();
        this.coerceByDefault = builder.coerce.getDefaultValue().value();
        this.scriptValues = builder.scriptValues();
        this.dimension = builder.dimension.getValue();
        this.builder = builder;
    }

    boolean coerce() {
        return coerce.value();
    }

    boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    @Override
    public NumberFieldType fieldType() {
        return (NumberFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType().type.typeName();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        Number value;
        try {
            value = value(context.parser(), type, nullValue, coerce.value());
        } catch (InputCoercionException | IllegalArgumentException | JsonParseException e) {
            if (ignoreMalformed.value() && context.parser().currentToken().isValue()) {
                context.addIgnoredField(mappedFieldType.name());
                return;
            } else {
                throw e;
            }
        }
        if (value != null) {
            indexValue(context, value);
        }
    }

    /**
     * Read the value at the current position of the parser.
     * @throws InputCoercionException if xcontent couldn't convert the value in the required type, for example, integer overflow
     * @throws JsonParseException if there was any error parsing the json
     * @throws IllegalArgumentException if there was an error parsing the value from the json
     * @throws IOException if there was any other IO error
     */
    private static Number value(XContentParser parser, NumberType numberType, Number nullValue, boolean coerce)
        throws InputCoercionException, JsonParseException, IllegalArgumentException, IOException {

        if (parser.currentToken() == Token.VALUE_NULL) {
            return nullValue;
        }
        if (coerce && parser.currentToken() == Token.VALUE_STRING && parser.textLength() == 0) {
            return nullValue;
        }
        return numberType.parse(parser, coerce);
    }

    private void indexValue(DocumentParserContext context, Number numericValue) {
        List<Field> fields = fieldType().type.createFields(fieldType().name(), numericValue, indexed, hasDocValues, stored);
        if (dimension) {
            // Check that a dimension field is single-valued and not an array
            if (context.doc().getByKey(fieldType().name()) != null) {
                throw new IllegalArgumentException("Dimension field [" + fieldType().name() + "] cannot be a multi-valued field.");
            }
            if (fields.size() > 0) {
                // Add the first field by key so that we can validate if it has been added
                context.doc().addWithKey(fieldType().name(), fields.get(0));
                context.doc().addAll(fields.subList(1, fields.size()));
            }
        } else {
            context.doc().addAll(fields);
        }

        if (hasDocValues == false && (stored || indexed)) {
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    protected void indexScriptValues(SearchLookup searchLookup, LeafReaderContext readerContext, int doc,
                                     DocumentParserContext documentParserContext) {
        this.scriptValues.valuesForDoc(searchLookup, readerContext, doc, value -> indexValue(documentParserContext, value));
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), type, builder.scriptCompiler, ignoreMalformedByDefault, coerceByDefault)
            .dimension(dimension).init(this);
    }
}
