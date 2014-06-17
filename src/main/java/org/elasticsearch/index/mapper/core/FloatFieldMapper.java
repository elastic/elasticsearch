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

import com.carrotsearch.hppc.FloatArrayList;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NumericFloatAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.NumericRangeFieldDataFilter;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeFloatValue;
import static org.elasticsearch.index.mapper.MapperBuilders.floatField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class FloatFieldMapper extends NumberFieldMapper<Float> {

    public static final String CONTENT_TYPE = "float";

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final FieldType FIELD_TYPE = new FieldType(NumberFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.freeze();
        }

        public static final Float NULL_VALUE = null;
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, FloatFieldMapper> {

        protected Float nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE), Defaults.PRECISION_STEP_32_BIT);
            builder = this;
        }

        public Builder nullValue(float nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public FloatFieldMapper build(BuilderContext context) {
            fieldType.setOmitNorms(fieldType.omitNorms() && boost == 1.0f);
            FloatFieldMapper fieldMapper = new FloatFieldMapper(buildNames(context),
                    fieldType.numericPrecisionStep(), boost, fieldType, docValues, nullValue, ignoreMalformed(context), coerce(context), postingsProvider, 
                    docValuesProvider, similarity, normsLoading, fieldDataSettings, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            FloatFieldMapper.Builder builder = floatField(name);
            parseNumberField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    builder.nullValue(nodeFloatValue(propNode));
                }
            }
            return builder;
        }
    }

    private Float nullValue;

    private String nullValueAsString;

    protected FloatFieldMapper(Names names, int precisionStep, float boost, FieldType fieldType, Boolean docValues,
                               Float nullValue, Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                               PostingsFormatProvider postingsProvider, DocValuesFormatProvider docValuesProvider,
                               SimilarityProvider similarity, Loading normsLoading, @Nullable Settings fieldDataSettings, 
                               Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(names, precisionStep, boost, fieldType, docValues, ignoreMalformed, coerce,
                NumericFloatAnalyzer.buildNamedAnalyzer(precisionStep), NumericFloatAnalyzer.buildNamedAnalyzer(Integer.MAX_VALUE),
                postingsProvider, docValuesProvider, similarity, normsLoading, fieldDataSettings, indexSettings, multiFields, copyTo);
        this.nullValue = nullValue;
        this.nullValueAsString = nullValue == null ? null : nullValue.toString();
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("float");
    }

    @Override
    protected int maxPrecisionStep() {
        return 32;
    }

    @Override
    public Float value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        if (value instanceof BytesRef) {
            return Numbers.bytesToFloat((BytesRef) value);
        }
        return Float.parseFloat(value.toString());
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        int intValue = NumericUtils.floatToSortableInt(parseValue(value));
        BytesRef bytesRef = new BytesRef();
        NumericUtils.intToPrefixCoded(intValue, 0, bytesRef);   // 0 because of exact match
        return bytesRef;
    }

    private float parseValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        if (value instanceof BytesRef) {
            return Float.parseFloat(((BytesRef) value).utf8ToString());
        }
        return Float.parseFloat(value.toString());
    }

    @Override
    public Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        float iValue = Float.parseFloat(value);
        final float iSim = fuzziness.asFloat();
        return NumericRangeQuery.newFloatRange(names.indexName(), precisionStep,
                iValue - iSim,
                iValue + iSim,
                true, true);
    }

    @Override
    public Query termQuery(Object value, @Nullable QueryParseContext context) {
        float fValue = parseValue(value);
        return NumericRangeQuery.newFloatRange(names.indexName(), precisionStep,
                fValue, fValue, true, true);
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeQuery.newFloatRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : parseValue(lowerTerm),
                upperTerm == null ? null : parseValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Filter termFilter(Object value, @Nullable QueryParseContext context) {
        float fValue = parseValue(value);
        return NumericRangeFilter.newFloatRange(names.indexName(), precisionStep,
                fValue, fValue, true, true);
    }

    @Override
    public Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeFilter.newFloatRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : parseValue(lowerTerm),
                upperTerm == null ? null : parseValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(IndexFieldDataService fieldData, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeFieldDataFilter.newFloatRange((IndexNumericFieldData) fieldData.getForField(this),
                lowerTerm == null ? null : parseValue(lowerTerm),
                upperTerm == null ? null : parseValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        return NumericRangeFilter.newFloatRange(names.indexName(), precisionStep,
                nullValue,
                nullValue,
                true, true);
    }

    @Override
    protected boolean customBoost() {
        return true;
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        float value;
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
                    value = Float.parseFloat(sExternalValue);
                }
            } else {
                value = ((Number) externalValue).floatValue();
            }
            if (context.includeInAll(includeInAll, this)) {
                context.allEntries().addText(names.fullName(), Float.toString(value), boost);
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
                Float objValue = nullValue;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                            if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                objValue = parser.floatValue(coerce.value());
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
                value = parser.floatValue(coerce.value());
                if (context.includeInAll(includeInAll, this)) {
                    context.allEntries().addText(names.fullName(), parser.text(), boost);
                }
            }
        }

        if (fieldType.indexed() || fieldType.stored()) {
            CustomFloatNumericField field = new CustomFloatNumericField(this, value, fieldType);
            field.setBoost(boost);
            fields.add(field);
        }
        if (hasDocValues()) {
            CustomFloatNumericDocValuesField field = (CustomFloatNumericDocValuesField) context.doc().getByKey(names().indexName());
            if (field != null) {
                field.add(value);
            } else {
                field = new CustomFloatNumericDocValuesField(names().indexName(), value);
                context.doc().addWithKey(names().indexName(), field);
            }
        }
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
            this.nullValue = ((FloatFieldMapper) mergeWith).nullValue;
            this.nullValueAsString = ((FloatFieldMapper) mergeWith).nullValueAsString;
        }
    }


    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || precisionStep != Defaults.PRECISION_STEP_32_BIT) {
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

    public static class CustomFloatNumericField extends CustomNumericField {

        private final float number;

        private final NumberFieldMapper mapper;

        public CustomFloatNumericField(NumberFieldMapper mapper, float number, FieldType fieldType) {
            super(mapper, number, fieldType);
            this.mapper = mapper;
            this.number = number;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer) throws IOException {
            if (fieldType().indexed()) {
                return mapper.popCachedStream().setFloatValue(number);
            }
            return null;
        }

        @Override
        public String numericAsString() {
            return Float.toString(number);
        }
    }

    public static class CustomFloatNumericDocValuesField extends CustomNumericDocValuesField {

        public static final FieldType TYPE = new FieldType();
        static {
          TYPE.setDocValueType(FieldInfo.DocValuesType.BINARY);
          TYPE.freeze();
        }

        private final FloatArrayList values;

        public CustomFloatNumericDocValuesField(String  name, float value) {
            super(name);
            values = new FloatArrayList();
            add(value);
        }

        public void add(float value) {
            values.add(value);
        }

        @Override
        public BytesRef binaryValue() {
            CollectionUtils.sortAndDedup(values);

            final byte[] bytes = new byte[values.size() * 4];
            for (int i = 0; i < values.size(); ++i) {
                ByteUtils.writeFloatLE(values.get(i), bytes, i * 4);
            }
            return new BytesRef(bytes);
        }

    }
}
