/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NumericDoubleAnalyzer;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.NumericRangeFieldDataFilter;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.index.mapper.MapperBuilders.doubleField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class DoubleFieldMapper extends NumberFieldMapper<Double> {

    public static final String CONTENT_TYPE = "double";

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final FieldType FIELD_TYPE = new FieldType(NumberFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.freeze();
        }

        public static final Double NULL_VALUE = null;
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, DoubleFieldMapper> {

        protected Double nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
            builder = this;
        }

        public Builder nullValue(double nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public DoubleFieldMapper build(BuilderContext context) {
            fieldType.setOmitNorms(fieldType.omitNorms() && boost == 1.0f);
            DoubleFieldMapper fieldMapper = new DoubleFieldMapper(buildNames(context),
                    precisionStep, boost, fieldType, nullValue,
                    ignoreMalformed(context), provider, similarity, fieldDataSettings);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            DoubleFieldMapper.Builder builder = doubleField(name);
            parseNumberField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("nullValue") || propName.equals("null_value")) {
                    builder.nullValue(nodeDoubleValue(propNode));
                }
            }
            return builder;
        }
    }


    private Double nullValue;

    private String nullValueAsString;

    protected DoubleFieldMapper(Names names, int precisionStep, float boost, FieldType fieldType,
                                Double nullValue, Explicit<Boolean> ignoreMalformed,
                                PostingsFormatProvider provider, SimilarityProvider similarity, @Nullable Settings fieldDataSettings) {
        super(names, precisionStep, boost, fieldType, ignoreMalformed,
                NumericDoubleAnalyzer.buildNamedAnalyzer(precisionStep), NumericDoubleAnalyzer.buildNamedAnalyzer(Integer.MAX_VALUE),
                provider, similarity, fieldDataSettings);
        this.nullValue = nullValue;
        this.nullValueAsString = nullValue == null ? null : nullValue.toString();
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("double");
    }

    @Override
    protected int maxPrecisionStep() {
        return 64;
    }

    @Override
    public Double value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof BytesRef) {
            return Numbers.bytesToDouble((BytesRef) value);
        }
        return Double.parseDouble(value.toString());
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        long longValue = NumericUtils.doubleToSortableLong(parseDoubleValue(value));
        BytesRef bytesRef = new BytesRef();
        NumericUtils.longToPrefixCoded(longValue, 0, bytesRef);   // 0 because of exact match
        return bytesRef;
    }

    @Override
    public Query fuzzyQuery(String value, String minSim, int prefixLength, int maxExpansions, boolean transpositions) {
        double iValue = Double.parseDouble(value);
        double iSim = Double.parseDouble(minSim);
        return NumericRangeQuery.newDoubleRange(names.indexName(), precisionStep,
                iValue - iSim,
                iValue + iSim,
                true, true);
    }

    @Override
    public Query termQuery(Object value, @Nullable QueryParseContext context) {
        double dValue = parseDoubleValue(value);
        return NumericRangeQuery.newDoubleRange(names.indexName(), precisionStep,
                dValue, dValue, true, true);
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeQuery.newDoubleRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : parseDoubleValue(lowerTerm),
                upperTerm == null ? null : parseDoubleValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Filter termFilter(Object value, @Nullable QueryParseContext context) {
        double dValue = parseDoubleValue(value);
        return NumericRangeFilter.newDoubleRange(names.indexName(), precisionStep,
                dValue, dValue, true, true);
    }

    @Override
    public Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeFilter.newDoubleRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : parseDoubleValue(lowerTerm),
                upperTerm == null ? null : parseDoubleValue(upperTerm),
                includeLower, includeUpper);
    }

    public Filter rangeFilter(Double lowerTerm, Double upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeFilter.newDoubleRange(names.indexName(), precisionStep, lowerTerm, upperTerm, includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(IndexFieldDataService fieldData, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeFieldDataFilter.newDoubleRange((IndexNumericFieldData) fieldData.getForField(this),
                lowerTerm == null ? null : parseDoubleValue(lowerTerm),
                upperTerm == null ? null : parseDoubleValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        return NumericRangeFilter.newDoubleRange(names.indexName(), precisionStep,
                nullValue,
                nullValue,
                true, true);
    }

    @Override
    protected boolean customBoost() {
        return true;
    }

    @Override
    protected Field innerParseCreateField(ParseContext context) throws IOException {
        double value;
        float boost = this.boost;
        if (context.externalValueSet()) {
            Object externalValue = context.externalValue();
            if (externalValue == null) {
                if (nullValue == null) {
                    return null;
                }
                value = nullValue;
            } else if (externalValue instanceof String) {
                String sExternalValue = (String) externalValue;
                if (sExternalValue.length() == 0) {
                    if (nullValue == null) {
                        return null;
                    }
                    value = nullValue;
                } else {
                    value = Double.parseDouble(sExternalValue);
                }
            } else {
                value = ((Number) externalValue).doubleValue();
            }
            if (context.includeInAll(includeInAll, this)) {
                context.allEntries().addText(names.fullName(), Double.toString(value), boost);
            }
        } else {
            XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL ||
                    (parser.currentToken() == XContentParser.Token.VALUE_STRING && parser.textLength() == 0)) {
                if (nullValue == null) {
                    return null;
                }
                value = nullValue;
                if (nullValueAsString != null && (context.includeInAll(includeInAll, this))) {
                    context.allEntries().addText(names.fullName(), nullValueAsString, boost);
                }
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                XContentParser.Token token;
                String currentFieldName = null;
                Double objValue = nullValue;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                            if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                objValue = parser.doubleValue();
                            }
                        } else if ("boost".equals(currentFieldName) || "_boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else {
                            throw new ElasticSearchIllegalArgumentException("unknown property [" + currentFieldName + "]");
                        }
                    }
                }
                if (objValue == null) {
                    // no value
                    return null;
                }
                value = objValue;
            } else {
                value = parser.doubleValue();
                if (context.includeInAll(includeInAll, this)) {
                    context.allEntries().addText(names.fullName(), parser.text(), boost);
                }
            }
        }

        CustomDoubleNumericField field = new CustomDoubleNumericField(this, value, fieldType);
        field.setBoost(boost);
        return field;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeContext.mergeFlags().simulate()) {
            this.nullValue = ((DoubleFieldMapper) mergeWith).nullValue;
            this.nullValueAsString = ((DoubleFieldMapper) mergeWith).nullValueAsString;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || precisionStep != Defaults.PRECISION_STEP) {
            builder.field("precision_step", precisionStep);
        }
        if (includeDefaults || nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", false);
        }

    }

    public static class CustomDoubleNumericField extends CustomNumericField {

        private final double number;

        private final NumberFieldMapper mapper;

        public CustomDoubleNumericField(NumberFieldMapper mapper, double number, FieldType fieldType) {
            super(mapper, number, fieldType);
            this.mapper = mapper;
            this.number = number;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer) throws IOException {
            if (fieldType().indexed()) {
                return mapper.popCachedStream().setDoubleValue(number);
            }
            return null;
        }

        @Override
        public String numericAsString() {
            return Double.toString(number);
        }
    }
}
