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

import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.NumberFieldMapper.Defaults;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** A {@link FieldMapper} for scaled floats. Values are internally multiplied
 *  by a scaling factor and rounded to the closest long. */
public class ScaledFloatFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "scaled_float";
    // use the same default as numbers
    private static final Setting<Boolean> COERCE_SETTING = NumberFieldMapper.COERCE_SETTING;

    public static class Builder extends FieldMapper.Builder<Builder> {

        private boolean scalingFactorSet = false;
        private Boolean ignoreMalformed;
        private Boolean coerce;

        public Builder(String name) {
            super(name, new ScaledFloatFieldType(), new ScaledFloatFieldType());
            builder = this;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        @Override
        public Builder indexOptions(IndexOptions indexOptions) {
            throw new MapperParsingException(
                    "index_options not allowed in field [" + name + "] of type [" + builder.fieldType().typeName() + "]");
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

        public Builder scalingFactor(double scalingFactor) {
            ((ScaledFloatFieldType) fieldType).setScalingFactor(scalingFactor);
            scalingFactorSet = true;
            return this;
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
        public ScaledFloatFieldMapper build(BuilderContext context) {
            if (scalingFactorSet == false) {
                throw new IllegalArgumentException("Field [" + name + "] misses required parameter [scaling_factor]");
            }
            setupFieldType(context);
            return new ScaledFloatFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context),
                    coerce(context), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node,
                                         ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            TypeParsers.parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(ScaledFloatFieldMapper.parse(propNode));
                    iterator.remove();
                } else if (propName.equals("ignore_malformed")) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode, name + ".ignore_malformed"));
                    iterator.remove();
                } else if (propName.equals("coerce")) {
                    builder.coerce(XContentMapValues.nodeBooleanValue(propNode, name + ".coerce"));
                    iterator.remove();
                } else if (propName.equals("scaling_factor")) {
                    builder.scalingFactor(ScaledFloatFieldMapper.parse(propNode));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class ScaledFloatFieldType extends SimpleMappedFieldType {

        private double scalingFactor;

        public ScaledFloatFieldType() {
            super();
            setTokenized(false);
            setHasDocValues(true);
            setOmitNorms(true);
        }

        ScaledFloatFieldType(ScaledFloatFieldType other) {
            super(other);
            this.scalingFactor = other.scalingFactor;
        }

        public double getScalingFactor() {
            return scalingFactor;
        }

        public void setScalingFactor(double scalingFactor) {
            checkIfFrozen();
            this.scalingFactor = scalingFactor;
        }

        @Override
        public MappedFieldType clone() {
            return new ScaledFloatFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            failIfNotIndexed();
            long scaledValue = Math.round(scale(value));
            Query query = NumberFieldMapper.NumberType.LONG.termQuery(name(), scaledValue);
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            failIfNotIndexed();
            List<Long> scaledValues = new ArrayList<>(values.size());
            for (Object value : values) {
                long scaledValue = Math.round(scale(value));
                scaledValues.add(scaledValue);
            }
            Query query = NumberFieldMapper.NumberType.LONG.termsQuery(name(), Collections.unmodifiableList(scaledValues));
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            failIfNotIndexed();
            Long lo = null;
            if (lowerTerm != null) {
                double dValue = scale(lowerTerm);
                if (includeLower == false) {
                    dValue = Math.nextUp(dValue);
                }
                lo = Math.round(Math.ceil(dValue));
            }
            Long hi = null;
            if (upperTerm != null) {
                double dValue = scale(upperTerm);
                if (includeUpper == false) {
                    dValue = Math.nextDown(dValue);
                }
                hi = Math.round(Math.floor(dValue));
            }
            Query query = NumberFieldMapper.NumberType.LONG.rangeQuery(name(), lo, hi, true, true, hasDocValues(), context);
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new IndexFieldData.Builder() {
                @Override
                public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                        CircuitBreakerService breakerService, MapperService mapperService) {
                    final IndexNumericFieldData scaledValues = new SortedNumericIndexFieldData.Builder(
                        IndexNumericFieldData.NumericType.LONG
                    )
                            .build(indexSettings, fieldType, cache, breakerService, mapperService);
                    return new ScaledFloatIndexFieldData(scaledValues, scalingFactor);
                }
            };
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            return CoreValuesSourceType.NUMERIC;
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return ((Number) value).longValue() / scalingFactor;
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

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            return scalingFactor == ((ScaledFloatFieldType) o).scalingFactor;
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode() + Double.hashCode(scalingFactor);
        }

        /**
         * Parses input value and multiplies it with the scaling factor.
         * Uses the round-trip of creating a {@link BigDecimal} from the stringified {@code double}
         * input to ensure intuitively exact floating point operations.
         * (e.g. for a scaling factor of 100, JVM behaviour results in {@code 79.99D * 100 ==> 7998.99..} compared to
         * {@code scale(79.99) ==> 7999})
         * @param input Input value to parse floating point num from
         * @return Scaled value
         */
        private double scale(Object input) {
            return new BigDecimal(Double.toString(parse(input))).multiply(BigDecimal.valueOf(scalingFactor)).doubleValue();
        }
    }

    private Explicit<Boolean> ignoreMalformed;

    private Explicit<Boolean> coerce;

    private ScaledFloatFieldMapper(
            String simpleName,
            MappedFieldType fieldType,
            MappedFieldType defaultFieldType,
            Explicit<Boolean> ignoreMalformed,
            Explicit<Boolean> coerce,
            Settings indexSettings,
            MultiFields multiFields,
            CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        final double scalingFactor = fieldType().getScalingFactor();
        if (Double.isFinite(scalingFactor) == false || scalingFactor <= 0) {
            throw new IllegalArgumentException("[scaling_factor] must be a positive number, got [" + scalingFactor + "]");
        }
        this.ignoreMalformed = ignoreMalformed;
        this.coerce = coerce;
    }

    @Override
    public ScaledFloatFieldType fieldType() {
        return (ScaledFloatFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType.typeName();
    }

    @Override
    protected ScaledFloatFieldMapper clone() {
        return (ScaledFloatFieldMapper) super.clone();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {

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
                numericValue = parse(parser, coerce.value());
            } catch (IllegalArgumentException e) {
                if (ignoreMalformed.value()) {
                    return;
                } else {
                    throw e;
                }
            }
            value = numericValue;
        }

        if (value == null) {
            value = fieldType().nullValue();
        }

        if (value == null) {
            return;
        }

        if (numericValue == null) {
            numericValue = parse(value);
        }

        double doubleValue = numericValue.doubleValue();
        if (Double.isFinite(doubleValue) == false) {
            if (ignoreMalformed.value()) {
                return;
            } else {
                // since we encode to a long, we have no way to carry NaNs and infinities
                throw new IllegalArgumentException("[scaled_float] only supports finite values, but got [" + doubleValue + "]");
            }
        }
        long scaledValue = Math.round(doubleValue * fieldType().getScalingFactor());

        boolean indexed = fieldType().indexOptions() != IndexOptions.NONE;
        boolean docValued = fieldType().hasDocValues();
        boolean stored = fieldType().stored();
        List<Field> fields = NumberFieldMapper.NumberType.LONG.createFields(fieldType().name(), scaledValue, indexed, docValued, stored);
        context.doc().addAll(fields);

        if (docValued == false && (indexed || stored)) {
            createFieldNamesField(context);
        }
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        ScaledFloatFieldMapper mergeWith = (ScaledFloatFieldMapper) other;
        ScaledFloatFieldType ft = (ScaledFloatFieldType) other.fieldType();
        if (fieldType().scalingFactor != ft.getScalingFactor()) {
            conflicts.add("mapper [" + name() + "] has different [scaling_factor] values");
        }
        if (mergeWith.ignoreMalformed.explicit()) {
            this.ignoreMalformed = mergeWith.ignoreMalformed;
        }
        if (mergeWith.coerce.explicit()) {
            this.coerce = mergeWith.coerce;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        builder.field("scaling_factor", fieldType().getScalingFactor());

        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field("ignore_malformed", ignoreMalformed.value());
        }
        if (includeDefaults || coerce.explicit()) {
            builder.field("coerce", coerce.value());
        }

        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field("null_value", fieldType().nullValue());
        }
    }

    static Double parse(Object value) {
        return objectToDouble(value);
    }

    private static Double parse(XContentParser parser, boolean coerce) throws IOException {
        return parser.doubleValue(coerce);
    }

    /**
     * Converts an Object to a double by checking it against known types first
     */
    private static double objectToDouble(Object value) {
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

    @Override
    protected Double parseSourceValue(Object value) {
        double doubleValue = objectToDouble(value);
        double scalingFactor = fieldType().getScalingFactor();
        return Math.round(doubleValue * scalingFactor) / scalingFactor;
    }

    private static class ScaledFloatIndexFieldData implements IndexNumericFieldData {

        private final IndexNumericFieldData scaledFieldData;
        private final double scalingFactor;

        ScaledFloatIndexFieldData(IndexNumericFieldData scaledFieldData, double scalingFactor) {
            this.scaledFieldData = scaledFieldData;
            this.scalingFactor = scalingFactor;
        }

        @Override
        public String getFieldName() {
            return scaledFieldData.getFieldName();
        }

        @Override
        public LeafNumericFieldData load(LeafReaderContext context) {
            return new ScaledFloatLeafFieldData(scaledFieldData.load(context), scalingFactor);
        }

        @Override
        public LeafNumericFieldData loadDirect(LeafReaderContext context) throws Exception {
            return new ScaledFloatLeafFieldData(scaledFieldData.loadDirect(context), scalingFactor);
        }

        @Override
        public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
            final XFieldComparatorSource source = new DoubleValuesComparatorSource(this, missingValue, sortMode, nested);
            return new SortField(getFieldName(), source, reverse);
        }

        @Override
        public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, Nested nested,
                SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
            return new DoubleValuesComparatorSource(this, missingValue, sortMode, nested)
                    .newBucketedSort(bigArrays, sortOrder, format, bucketSize, extra);
        }

        @Override
        public void clear() {
            scaledFieldData.clear();
        }

        @Override
        public Index index() {
            return scaledFieldData.index();
        }

        @Override
        public NumericType getNumericType() {
            /**
             * {@link ScaledFloatLeafFieldData#getDoubleValues()} transforms the raw long values in `scaled` floats.
             */
            return NumericType.DOUBLE;
        }

    }

    private static class ScaledFloatLeafFieldData implements LeafNumericFieldData {

        private final LeafNumericFieldData scaledFieldData;
        private final double scalingFactorInverse;

        ScaledFloatLeafFieldData(LeafNumericFieldData scaledFieldData, double scalingFactor) {
            this.scaledFieldData = scaledFieldData;
            this.scalingFactorInverse = 1d / scalingFactor;
        }

        @Override
        public ScriptDocValues.Doubles getScriptValues() {
            return new ScriptDocValues.Doubles(getDoubleValues());
        }

        @Override
        public SortedBinaryDocValues getBytesValues() {
            return FieldData.toString(getDoubleValues());
        }

        @Override
        public long ramBytesUsed() {
            return scaledFieldData.ramBytesUsed();
        }

        @Override
        public void close() {
            scaledFieldData.close();
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return FieldData.castToLong(getDoubleValues());
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            final SortedNumericDocValues values = scaledFieldData.getLongValues();
            final NumericDocValues singleValues = DocValues.unwrapSingleton(values);
            if (singleValues != null) {
                return FieldData.singleton(new NumericDoubleValues() {
                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return singleValues.advanceExact(doc);
                    }
                    @Override
                    public double doubleValue() throws IOException {
                        return singleValues.longValue() * scalingFactorInverse;
                    }
                });
            } else {
                return new SortedNumericDoubleValues() {

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        return values.advanceExact(target);
                    }

                    @Override
                    public double nextValue() throws IOException {
                        return values.nextValue() * scalingFactorInverse;
                    }

                    @Override
                    public int docValueCount() {
                        return values.docValueCount();
                    }
                };
            }
        }

    }
}
