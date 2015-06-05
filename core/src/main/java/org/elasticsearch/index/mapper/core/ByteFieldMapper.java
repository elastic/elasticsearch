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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericIntegerAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeByteValue;
import static org.elasticsearch.index.mapper.MapperBuilders.byteField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class ByteFieldMapper extends NumberFieldMapper {

    public static final String CONTENT_TYPE = "byte";

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final MappedFieldType FIELD_TYPE = new ByteFieldType();

        static {
            FIELD_TYPE.freeze();
        }

        public static final Byte NULL_VALUE = null;
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, ByteFieldMapper> {

        protected Byte nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.PRECISION_STEP_8_BIT);
            builder = this;
        }

        public Builder nullValue(byte nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public ByteFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            ByteFieldMapper fieldMapper = new ByteFieldMapper(fieldType, docValues, nullValue, ignoreMalformed(context),
                    coerce(context), fieldDataSettings, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }

        @Override
        protected NamedAnalyzer makeNumberAnalyzer(int precisionStep) {
            String name = precisionStep == Integer.MAX_VALUE ? "_byte/max" : ("_byte/" + precisionStep);
            return new NamedAnalyzer(name, new NumericIntegerAnalyzer(precisionStep));
        }

        @Override
        protected int maxPrecisionStep() {
            return 32;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            ByteFieldMapper.Builder builder = byteField(name);
            parseNumberField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(nodeByteValue(propNode));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    static final class ByteFieldType extends NumberFieldType {
        public ByteFieldType() {}

        protected ByteFieldType(ByteFieldType ref) {
            super(ref);
        }

        @Override
        public NumberFieldType clone() {
            return new ByteFieldType(this);
        }

        @Override
        public Byte value(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof Number) {
                return ((Number) value).byteValue();
            }
            if (value instanceof BytesRef) {
                return ((BytesRef) value).bytes[((BytesRef) value).offset];
            }
            return Byte.parseByte(value.toString());
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            BytesRefBuilder bytesRef = new BytesRefBuilder();
            NumericUtils.intToPrefixCoded(parseValue(value), 0, bytesRef); // 0 because of exact match
            return bytesRef.get();
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
            return NumericRangeQuery.newIntRange(names().indexName(), numericPrecisionStep(),
                lowerTerm == null ? null : (int)parseValue(lowerTerm),
                upperTerm == null ? null : (int)parseValue(upperTerm),
                includeLower, includeUpper);
        }

        @Override
        public Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
            byte iValue = Byte.parseByte(value);
            byte iSim = fuzziness.asByte();
            return NumericRangeQuery.newIntRange(names().indexName(), numericPrecisionStep(),
                iValue - iSim,
                iValue + iSim,
                true, true);
        }

        @Override
        public FieldStats stats(Terms terms, int maxDoc) throws IOException {
            long minValue = NumericUtils.getMinInt(terms);
            long maxValue = NumericUtils.getMaxInt(terms);
            return new FieldStats.Long(
                maxDoc, terms.getDocCount(), terms.getSumDocFreq(), terms.getSumTotalTermFreq(), minValue, maxValue
            );
        }
    }

    private Byte nullValue;

    private String nullValueAsString;

    protected ByteFieldMapper(MappedFieldType fieldType, Boolean docValues,
                              Byte nullValue, Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                              @Nullable Settings fieldDataSettings, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(fieldType, docValues, ignoreMalformed, coerce, fieldDataSettings, indexSettings, multiFields, copyTo);
        this.nullValue = nullValue;
        this.nullValueAsString = nullValue == null ? null : nullValue.toString();
    }

    @Override
    public MappedFieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("byte");
    }

    private static byte parseValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }
        if (value instanceof BytesRef) {
            return Byte.parseByte(((BytesRef) value).utf8ToString());
        }
        return Byte.parseByte(value.toString());
    }

    @Override
    public Query nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        return new ConstantScoreQuery(termQuery(nullValue, null));
    }

    @Override
    protected boolean customBoost() {
        return true;
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        byte value;
        float boost = this.fieldType.boost();
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
                    value = Byte.parseByte(sExternalValue);
                }
            } else {
                value = ((Number) externalValue).byteValue();
            }
            if (context.includeInAll(includeInAll, this)) {
                context.allEntries().addText(fieldType.names().fullName(), Byte.toString(value), boost);
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
                    context.allEntries().addText(fieldType.names().fullName(), nullValueAsString, boost);
                }
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                XContentParser.Token token;
                String currentFieldName = null;
                Byte objValue = nullValue;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                            if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                objValue = (byte) parser.shortValue(coerce.value());
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
                value = (byte) parser.shortValue(coerce.value());
                if (context.includeInAll(includeInAll, this)) {
                    context.allEntries().addText(fieldType.names().fullName(), parser.text(), boost);
                }
            }
        }
        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            CustomByteNumericField field = new CustomByteNumericField(this, value, fieldType);
            field.setBoost(boost);
            fields.add(field);
        }
        if (fieldType().hasDocValues()) {
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
            this.nullValue = ((ByteFieldMapper) mergeWith).nullValue;
            this.nullValueAsString = ((ByteFieldMapper) mergeWith).nullValueAsString;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || fieldType.numericPrecisionStep() != Defaults.PRECISION_STEP_8_BIT) {
            builder.field("precision_step", fieldType.numericPrecisionStep());
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

    public static class CustomByteNumericField extends CustomNumericField {

        private final byte number;

        private final NumberFieldMapper mapper;

        public CustomByteNumericField(NumberFieldMapper mapper, byte number, MappedFieldType fieldType) {
            super(mapper, number, fieldType);
            this.mapper = mapper;
            this.number = number;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream previous) {
            if (fieldType().indexOptions() != IndexOptions.NONE) {
                return mapper.popCachedStream().setIntValue(number);
            }
            return null;
        }

        @Override
        public String numericAsString() {
            return Byte.toString(number);
        }
    }
}
