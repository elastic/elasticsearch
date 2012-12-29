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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NamedCustomAnalyzer;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.stringField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
public class StringFieldMapper extends AbstractFieldMapper<String> implements AllFieldMapper.IncludeInAll {

    public static final String CONTENT_TYPE = "string";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.freeze();
        }

        // NOTE, when adding defaults here, make sure you add them in the builder
        public static final String NULL_VALUE = null;
        public static final int POSITION_OFFSET_GAP = 0;
        public static final int IGNORE_ABOVE = -1;
    }

    public static class Builder extends AbstractFieldMapper.OpenBuilder<Builder, StringFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        protected int positionOffsetGap = Defaults.POSITION_OFFSET_GAP;

        protected NamedAnalyzer searchQuotedAnalyzer;

        protected int ignoreAbove = Defaults.IGNORE_ABOVE;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
            builder = this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public Builder includeInAll(Boolean includeInAll) {
            this.includeInAll = includeInAll;
            return this;
        }

        @Override
        public Builder searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            super.searchAnalyzer(searchAnalyzer);
            if (searchQuotedAnalyzer == null) {
                searchQuotedAnalyzer = searchAnalyzer;
            }
            return this;
        }

        public Builder positionOffsetGap(int positionOffsetGap) {
            this.positionOffsetGap = positionOffsetGap;
            return this;
        }

        public Builder searchQuotedAnalyzer(NamedAnalyzer analyzer) {
            this.searchQuotedAnalyzer = analyzer;
            return builder;
        }

        public Builder ignoreAbove(int ignoreAbove) {
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        @Override
        public StringFieldMapper build(BuilderContext context) {
            if (positionOffsetGap > 0) {
                indexAnalyzer = new NamedCustomAnalyzer(indexAnalyzer, positionOffsetGap);
                searchAnalyzer = new NamedCustomAnalyzer(searchAnalyzer, positionOffsetGap);
                searchQuotedAnalyzer = new NamedCustomAnalyzer(searchQuotedAnalyzer, positionOffsetGap);
            }
            // if the field is not analyzed, then by default, we should omit norms and have docs only
            // index options, as probably what the user really wants
            // if they are set explicitly, we will use those values
            if (fieldType.indexed() && !fieldType.tokenized()) {
                if (!omitNormsSet && boost == Defaults.BOOST) {
                    fieldType.setOmitNorms(true);
                }
                if (!indexOptionsSet) {
                    fieldType.setIndexOptions(IndexOptions.DOCS_ONLY);
                }
            }
            StringFieldMapper fieldMapper = new StringFieldMapper(buildNames(context),
                    boost, fieldType, nullValue, indexAnalyzer, searchAnalyzer, searchQuotedAnalyzer,
                    positionOffsetGap, ignoreAbove, provider, similarity);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            StringFieldMapper.Builder builder = stringField(name);
            parseField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    builder.nullValue(propNode.toString());
                } else if (propName.equals("search_quote_analyzer")) {
                    NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                    if (analyzer == null) {
                        throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                    }
                    builder.searchQuotedAnalyzer(analyzer);
                } else if (propName.equals("position_offset_gap")) {
                    builder.positionOffsetGap(XContentMapValues.nodeIntegerValue(propNode, -1));
                    // we need to update to actual analyzers if they are not set in this case...
                    // so we can inject the position offset gap...
                    if (builder.indexAnalyzer == null) {
                        builder.indexAnalyzer = parserContext.analysisService().defaultIndexAnalyzer();
                    }
                    if (builder.searchAnalyzer == null) {
                        builder.searchAnalyzer = parserContext.analysisService().defaultSearchAnalyzer();
                    }
                    if (builder.searchQuotedAnalyzer == null) {
                        builder.searchQuotedAnalyzer = parserContext.analysisService().defaultSearchQuoteAnalyzer();
                    }
                } else if (propName.equals("ignore_above")) {
                    builder.ignoreAbove(XContentMapValues.nodeIntegerValue(propNode, -1));
                }
            }
            return builder;
        }
    }

    private String nullValue;

    private Boolean includeInAll;

    private int positionOffsetGap;

    private NamedAnalyzer searchQuotedAnalyzer;

    private int ignoreAbove;

    protected StringFieldMapper(Names names, float boost, FieldType fieldType,
                                String nullValue, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer,
                                PostingsFormatProvider postingsFormat, SimilarityProvider similarity) {
        this(names, boost, fieldType, nullValue, indexAnalyzer, searchAnalyzer, searchAnalyzer,
                Defaults.POSITION_OFFSET_GAP, Defaults.IGNORE_ABOVE, postingsFormat, similarity);
    }

    protected StringFieldMapper(Names names, float boost, FieldType fieldType,
                                String nullValue, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer,
                                NamedAnalyzer searchQuotedAnalyzer, int positionOffsetGap, int ignoreAbove,
                                PostingsFormatProvider postingsFormat, SimilarityProvider similarity) {
        super(names, boost, fieldType, indexAnalyzer, searchAnalyzer, postingsFormat, similarity);
        this.nullValue = nullValue;
        this.positionOffsetGap = positionOffsetGap;
        this.searchQuotedAnalyzer = searchQuotedAnalyzer != null ? searchQuotedAnalyzer : this.searchAnalyzer;
        this.ignoreAbove = ignoreAbove;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public void includeInAll(Boolean includeInAll) {
        if (includeInAll != null) {
            this.includeInAll = includeInAll;
        }
    }

    @Override
    public void includeInAllIfNotSet(Boolean includeInAll) {
        if (includeInAll != null && this.includeInAll == null) {
            this.includeInAll = includeInAll;
        }
    }

    @Override
    public String value(Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    @Override
    protected boolean customBoost() {
        return true;
    }

    public int getPositionOffsetGap() {
        return this.positionOffsetGap;
    }

    @Override
    public Analyzer searchQuoteAnalyzer() {
        return this.searchQuotedAnalyzer;
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        return termFilter(nullValue, null);
    }

    @Override
    protected Field parseCreateField(ParseContext context) throws IOException {
        String value = nullValue;
        float boost = this.boost;
        if (context.externalValueSet()) {
            value = (String) context.externalValue();
        } else {
            XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                value = nullValue;
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                XContentParser.Token token;
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                            value = parser.textOrNull();
                        } else if ("boost".equals(currentFieldName) || "_boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        }
                    }
                }
            } else {
                value = parser.textOrNull();
            }
        }
        if (value == null) {
            return null;
        }
        if (ignoreAbove > 0 && value.length() > ignoreAbove) {
            return null;
        }
        if (context.includeInAll(includeInAll, this)) {
            context.allEntries().addText(names.fullName(), value, boost);
        }
        if (!fieldType().indexed() && !fieldType().stored()) {
            context.ignoredValue(names.indexName(), value);
            return null;
        }
        Field field = new StringField(names.indexName(), value, fieldType);
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
            this.includeInAll = ((StringFieldMapper) mergeWith).includeInAll;
            this.nullValue = ((StringFieldMapper) mergeWith).nullValue;
            this.ignoreAbove = ((StringFieldMapper) mergeWith).ignoreAbove;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder) throws IOException {
        super.doXContentBody(builder);
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        }
        if (positionOffsetGap != Defaults.POSITION_OFFSET_GAP) {
            builder.field("position_offset_gap", positionOffsetGap);
        }
        if (searchQuotedAnalyzer != null && searchAnalyzer != searchQuotedAnalyzer) {
            builder.field("search_quote_analyzer", searchQuotedAnalyzer.name());
        }
        if (ignoreAbove != Defaults.IGNORE_ABOVE) {
            builder.field("ignore_above", ignoreAbove);
        }
    }

    /**
     * Extension of {@link Field} supporting reuse of a cached TokenStream for not-tokenized values.
     */
    static class StringField extends Field {

        public StringField(String name, String value, FieldType fieldType) {
            super(name, value, fieldType);
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer) throws IOException {
            if (!fieldType().indexed()) {
                return null;
            }
            // Only use the cached TokenStream if the value is indexed and not-tokenized
            if (fieldType().tokenized()) {
                return super.tokenStream(analyzer);
            }
            return NOT_ANALYZED_TOKENSTREAM.get().setValue((String) fieldsData);
        }
    }

    private static final ThreadLocal<StringTokenStream> NOT_ANALYZED_TOKENSTREAM = new ThreadLocal<StringTokenStream>() {
        @Override
        protected StringTokenStream initialValue() {
            return new StringTokenStream();
        }
    };


    // Copied from Field.java
    static final class StringTokenStream extends TokenStream {
        private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
        private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
        private boolean used = false;
        private String value = null;

        /**
         * Creates a new TokenStream that returns a String as single token.
         * <p>Warning: Does not initialize the value, you must call
         * {@link #setValue(String)} afterwards!
         */
        StringTokenStream() {
        }

        /**
         * Sets the string value.
         */
        StringTokenStream setValue(String value) {
            this.value = value;
            return this;
        }

        @Override
        public boolean incrementToken() {
            if (used) {
                return false;
            }
            clearAttributes();
            termAttribute.append(value);
            offsetAttribute.setOffset(0, value.length());
            used = true;
            return true;
        }

        @Override
        public void end() {
            final int finalOffset = value.length();
            offsetAttribute.setOffset(finalOffset, finalOffset);
            value = null;
        }

        @Override
        public void reset() {
            used = false;
        }

        @Override
        public void close() {
            value = null;
        }
    }
}
