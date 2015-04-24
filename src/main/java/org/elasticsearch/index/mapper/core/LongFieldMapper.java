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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NumericLongAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.NumericRangeFieldDataFilter;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;
import static org.elasticsearch.index.mapper.MapperBuilders.longField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class LongFieldMapper extends NumberFieldMapper<Long> {

    public static final String CONTENT_TYPE = "long";

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final FieldType FIELD_TYPE = new FieldType(NumberFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.freeze();
        }

        public static final Long NULL_VALUE = null;
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, LongFieldMapper> {

        protected Long nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE), Defaults.PRECISION_STEP_64_BIT);
            builder = this;
        }

        public Builder nullValue(long nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public LongFieldMapper build(BuilderContext context) {
            fieldType.setOmitNorms(fieldType.omitNorms() && boost == 1.0f);
            LongFieldMapper fieldMapper = new LongFieldMapper(buildNames(context), fieldType.numericPrecisionStep(), boost, fieldType, docValues, nullValue,
                    ignoreMalformed(context), coerce(context), similarity, normsLoading, 
                    fieldDataSettings, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            LongFieldMapper.Builder builder = longField(name);
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
                }
            }
            return builder;
        }
    }

    private Long nullValue;

    private String nullValueAsString;

    protected LongFieldMapper(Names names, int precisionStep, float boost, FieldType fieldType, Boolean docValues,
                              Long nullValue, Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                              SimilarityProvider similarity, Loading normsLoading, @Nullable Settings fieldDataSettings, 
                              Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(names, precisionStep, boost, fieldType, docValues, ignoreMalformed, coerce,
                NumericLongAnalyzer.buildNamedAnalyzer(precisionStep), NumericLongAnalyzer.buildNamedAnalyzer(Integer.MAX_VALUE),
                similarity, normsLoading, fieldDataSettings, indexSettings, multiFields, copyTo);
        this.nullValue = nullValue;
        this.nullValueAsString = nullValue == null ? null : nullValue.toString();
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("long");
    }

    @Override
    protected int maxPrecisionStep() {
        return 64;
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
        BytesRefBuilder bytesRef = new BytesRefBuilder();
        NumericUtils.longToPrefixCoded(parseLongValue(value), 0, bytesRef);  // 0 because of exact match
        return bytesRef.get();
    }

    @Override
    public Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        long iValue = Long.parseLong(value);
        final long iSim = fuzziness.asLong();
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                iValue - iSim,
                iValue + iSim,
                true, true);
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : parseLongValue(lowerTerm),
                upperTerm == null ? null : parseLongValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return Queries.wrap(NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : parseLongValue(lowerTerm),
                upperTerm == null ? null : parseLongValue(upperTerm),
                includeLower, includeUpper));
    }

    @Override
    public Filter rangeFilter(QueryParseContext parseContext, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeFieldDataFilter.newLongRange((IndexNumericFieldData) parseContext.getForField(this),
                lowerTerm == null ? null : parseLongValue(lowerTerm),
                upperTerm == null ? null : parseLongValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        return Queries.wrap(NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                nullValue,
                nullValue,
                true, true));
    }

    @Override
    protected boolean customBoost() {
        return true;
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        long value;
        float boost = this.boost;
        if (context.externalValueSet()) {
            Object externalValue = context.externalValue();
            if (externalValue == null) {
                if (nullValue == null) {
                    return;
                }
                value = nullValue;
            } else if (externalValue instanceof String) {
                String sExternalValue = (String) externalValue;
                if (sExternalValue.length() == 0) {
                    if (nullValue == null) {
                        return;
                    }
                    value = nullValue;
                } else {
                    value = Long.parseLong(sExternalValue);
                }
            } else {
                value = ((Number) externalValue).longValue();
            }
            if (context.includeInAll(includeInAll, this)) {
                context.allEntries().addText(names.fullName(), Long.toString(value), boost);
            }
        } else {
            XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL ||
                    (parser.currentToken() == XContentParser.Token.VALUE_STRING && parser.textLength() == 0)) {
                if (nullValue == null) {
                    return;
                }
                value = nullValue;
                if (nullValueAsString != null && (context.includeInAll(includeInAll, this))) {
                    context.allEntries().addText(names.fullName(), nullValueAsString, boost);
                }
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                XContentParser.Token token;
                String currentFieldName = null;
                Long objValue = nullValue;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                            if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                objValue = parser.longValue(coerce.value());
                            }
                        } else if ("boost".equals(currentFieldName) || "_boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else {
                            throw new ElasticsearchIllegalArgumentException("unknown property [" + currentFieldName + "]");
                        }
                    }
                }
                if (objValue == null) {
                    // no value
                    return;
                }
                value = objValue;
            } else {
                value = parser.longValue(coerce.value());
                if (context.includeInAll(includeInAll, this)) {
                    context.allEntries().addText(names.fullName(), parser.text(), boost);
                }
            }
        }
        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            CustomLongNumericField field = new CustomLongNumericField(this, value, fieldType);
            field.setBoost(boost);
            fields.add(field);
        }
        if (hasDocValues()) {
            addDocValue(context, fields, value);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        super.merge(mergeWith, mergeResult);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeResult.simulate()) {
            this.nullValue = ((LongFieldMapper) mergeWith).nullValue;
            this.nullValueAsString = ((LongFieldMapper) mergeWith).nullValueAsString;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || precisionStep != Defaults.PRECISION_STEP_64_BIT) {
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

    @Override
    public FieldStats stats(Terms terms, int maxDoc) throws IOException {
        long minValue = NumericUtils.getMinLong(terms);
        long maxValue = NumericUtils.getMaxLong(terms);
        return new FieldStats.Long(
                maxDoc, terms.getDocCount(), terms.getSumDocFreq(), terms.getSumTotalTermFreq(), minValue, maxValue
        );
    }

    public static class CustomLongNumericField extends CustomNumericField {

        private final long number;

        private final NumberFieldMapper mapper;

        public CustomLongNumericField(NumberFieldMapper mapper, long number, FieldType fieldType) {
            super(mapper, number, fieldType);
            this.mapper = mapper;
            this.number = number;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream previous) throws IOException {
            if (fieldType().indexOptions() != IndexOptions.NONE) {
                return mapper.popCachedStream().setLongValue(number);
            }
            return null;
        }

        @Override
        public String numericAsString() {
            return Long.toString(number);
        }
    }
}
