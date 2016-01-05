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

package org.elasticsearch.index.mapper.fixed;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericLongAnalyzer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;
import static org.elasticsearch.index.mapper.MapperBuilders.fixedPointField;
import static org.elasticsearch.index.mapper.MapperBuilders.longField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class FixedPointFieldMapper extends NumberFieldMapper {

    public static final String CONTENT_TYPE = "fixed";

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final MappedFieldType FIELD_TYPE = new FixedPointFieldType();
        public static final long DECIMAL_PLACES = 2;
        public static final long DECIMAL_FACTOR = 100;

        static {
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, FixedPointFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.PRECISION_STEP_64_BIT);
            builder = this;
        }

        public Builder nullValue(long nullValue) {
            this.fieldType.setNullValue(nullValue);
            return this;
        }

        public Builder decimalPlaces(long decimalPlaces) {
            fieldType().setDecimalPlaces(decimalPlaces);
            return this;
        }

        @Override
        public FixedPointFieldType fieldType() {
            return (FixedPointFieldType)fieldType;
        }

        @Override
        public FixedPointFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            FixedPointFieldMapper fieldMapper = new FixedPointFieldMapper(name, fieldType, defaultFieldType,
                ignoreMalformed(context), coerce(context), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            return (FixedPointFieldMapper) fieldMapper.includeInAll(includeInAll);
        }

        @Override
        protected NamedAnalyzer makeNumberAnalyzer(int precisionStep) {
            return NumericLongAnalyzer.buildNamedAnalyzer(precisionStep);
        }

        @Override
        protected int maxPrecisionStep() {
            return 64;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            FixedPointFieldMapper.Builder builder = fixedPointField(name);
            parseNumberField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(nodeLongValue(propNode));
                    iterator.remove();
                } else if (propName.equals("decimal_places")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [decimal_places] cannot be null.");
                    }
                    long decimalPlaces = nodeLongValue(propNode);
                    if (decimalPlaces <= 0) {
                        throw new MapperParsingException("Property [decimal_places] must be greater than zero.");
                    }
                    if (decimalPlaces >= 19) {
                        // would cause an overflow in decimalFactor later
                        //
                        // Long.MAX_VALUE ==  9223372036854775807
                        // 10^19          == 10000000000000000000
                        throw new MapperParsingException("Property [decimal_places] must be smaller than 19.");
                    }

                    builder.decimalPlaces((byte)decimalPlaces);
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static class FixedPointFieldType extends NumberFieldType {
        private long decimalPlaces = Defaults.DECIMAL_PLACES;
        private long decimalFactor = Defaults.DECIMAL_FACTOR;

        public FixedPointFieldType() {
            super(NumericType.LONG);
        }

        protected FixedPointFieldType(FixedPointFieldType ref) {
            super(ref);
        }

        public void setDecimalPlaces(long decimalPlaces) {
            this.decimalPlaces = decimalPlaces;
            this.decimalFactor = (long) Math.pow(10.0, decimalPlaces);
        }

        public long getDecimalPlaces() {
            return this.decimalPlaces;
        }

        public long getDecimalFactor() {
            return this.decimalFactor;
        }

        @Override
        public NumberFieldType clone() {
            return new FixedPointFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Double nullValue() {
            return (Double)super.nullValue();
        }

        @Override
        public Long value(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            if (value instanceof BytesRef) {
                return Numbers.bytesToLong((BytesRef) value);
            }
            return Long.parseLong(value.toString());
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            long longValue = (long)(parseDoubleValue(value) * decimalFactor);
            BytesRefBuilder bytesRef = new BytesRefBuilder();
            NumericUtils.longToPrefixCoded(longValue, 0, bytesRef);   // 0 because of exact match
            return bytesRef.get();
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper) {
            return NumericRangeQuery.newLongRange(name(), numericPrecisionStep(),
                lowerTerm == null ? null : (long) (parseDoubleValue(lowerTerm) * decimalFactor),
                upperTerm == null ? null : (long) (parseDoubleValue(upperTerm) * decimalFactor),
                includeLower, includeUpper);
        }

        @Override
        public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
            final long iValue = (long)(parseDoubleValue(value) * decimalFactor);
            final long iSim = fuzziness.asLong();
            return NumericRangeQuery.newLongRange(name(), numericPrecisionStep(),
                iValue - iSim,
                iValue + iSim,
                true, true);
        }

        @Override
        public FieldStats stats(Terms terms, int maxDoc) throws IOException {
            long minValue = NumericUtils.getMinLong(terms);
            long maxValue = NumericUtils.getMaxLong(terms);
            return new FieldStats.Long(
                maxDoc, terms.getDocCount(), terms.getSumDocFreq(), terms.getSumTotalTermFreq(), minValue, maxValue
            );
        }
    }

    protected FixedPointFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                              Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                              Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, indexSettings, multiFields, copyTo);
    }

    @Override
    public FixedPointFieldType fieldType() {
        return (FixedPointFieldType) super.fieldType();
    }

    @Override
    protected boolean customBoost() {
        return true;
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        double value;
        float boost = fieldType().boost();
        if (context.externalValueSet()) {
            Object externalValue = context.externalValue();
            if (externalValue == null) {
                if (fieldType().nullValue() == null) {
                    return;
                }
                value = fieldType().nullValue();
            } else if (externalValue instanceof String) {
                String sExternalValue = (String) externalValue;
                if (sExternalValue.length() == 0) {
                    if (fieldType().nullValue() == null) {
                        return;
                    }
                    value = fieldType().nullValue();
                } else {
                    value = Double.parseDouble(sExternalValue);
                }
            } else {
                value = ((Number) externalValue).doubleValue();
            }
            if (context.includeInAll(includeInAll, this)) {
                context.allEntries().addText(fieldType().name(), Double.toString(value), boost);
            }
        } else {
            XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL ||
                (parser.currentToken() == XContentParser.Token.VALUE_STRING && parser.textLength() == 0)) {
                if (fieldType().nullValue() == null) {
                    return;
                }
                value = fieldType().nullValue();
                if (fieldType().nullValueAsString() != null && (context.includeInAll(includeInAll, this))) {
                    context.allEntries().addText(fieldType().name(), fieldType().nullValueAsString(), boost);
                }
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT
                && Version.indexCreated(context.indexSettings()).before(Version.V_3_0_0)) {
                XContentParser.Token token;
                String currentFieldName = null;
                Double objValue = fieldType().nullValue();
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                            if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                objValue = parser.doubleValue(coerce.value());
                            }
                        } else if ("boost".equals(currentFieldName) || "_boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else {
                            throw new IllegalArgumentException("unknown property [" + currentFieldName + "]");
                        }
                    }
                }
                if (objValue == null) {
                    // no value
                    return;
                }
                value = objValue;
            } else {
                value = parser.doubleValue(coerce.value());
                if (context.includeInAll(includeInAll, this)) {
                    context.allEntries().addText(fieldType().name(), parser.text(), boost);
                }
            }
        }
        final long scaledValue = (long)(value * (double)fieldType().decimalFactor);
        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            CustomFixedPointNumericField field = new CustomFixedPointNumericField(scaledValue, fieldType().decimalFactor, fieldType());
            field.setBoost(boost);
            fields.add(field);
        }
        if (fieldType().hasDocValues()) {
            addDocValue(context, fields, scaledValue);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || fieldType().numericPrecisionStep() != Defaults.PRECISION_STEP_64_BIT) {
            builder.field("precision_step", fieldType().numericPrecisionStep());
        }
        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field("null_value", fieldType().nullValue());
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", false);
        }
        builder.field("decimal_places", fieldType().decimalPlaces);
    }

    public static class CustomFixedPointNumericField extends CustomNumericField {

        private final long number;
        private final long decimalFactor;

        public CustomFixedPointNumericField(long number, long decimalFactor, MappedFieldType fieldType) {
            super(number, fieldType);
            this.number = number;
            this.decimalFactor = decimalFactor;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream previous) throws IOException {
            if (fieldType().indexOptions() != IndexOptions.NONE) {
                return getCachedStream().setLongValue(number);
            }
            return null;
        }

        @Override
        public String numericAsString() {
            return Double.toString(number / decimalFactor);
        }
    }
}
