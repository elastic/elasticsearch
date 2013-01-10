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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.Filter;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NamedCustomAnalyzer;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;

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
            super(name);
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
            if (index == Field.Index.NOT_ANALYZED) {
                if (!omitNormsSet) {
                    omitNorms = true;
                }
                if (!indexOptionsSet) {
                    indexOptions = IndexOptions.DOCS_ONLY;
                }
            }
            StringFieldMapper fieldMapper = new StringFieldMapper(buildNames(context),
                    index, store, termVector, boost, omitNorms, indexOptions, nullValue,
                    indexAnalyzer, searchAnalyzer, searchQuotedAnalyzer, positionOffsetGap, ignoreAbove);
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

    protected StringFieldMapper(Names names, Field.Index index, Field.Store store, Field.TermVector termVector,
                                float boost, boolean omitNorms, IndexOptions indexOptions,
                                String nullValue, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer) {
        this(names, index, store, termVector, boost, omitNorms, indexOptions, nullValue, indexAnalyzer,
                searchAnalyzer, searchAnalyzer, Defaults.POSITION_OFFSET_GAP, Defaults.IGNORE_ABOVE);
    }

    protected StringFieldMapper(Names names, Field.Index index, Field.Store store, Field.TermVector termVector,
                                float boost, boolean omitNorms, IndexOptions indexOptions,
                                String nullValue, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer,
                                NamedAnalyzer searchQuotedAnalyzer, int positionOffsetGap, int ignoreAbove) {
        super(names, index, store, termVector, boost, omitNorms, indexOptions, indexAnalyzer, searchAnalyzer);
        this.nullValue = nullValue;
        this.positionOffsetGap = positionOffsetGap;
        this.searchQuotedAnalyzer = searchQuotedAnalyzer != null ? searchQuotedAnalyzer : this.searchAnalyzer;
        this.ignoreAbove = ignoreAbove;
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
    public String value(Fieldable field) {
        return field.stringValue();
    }

    @Override
    public String valueFromString(String value) {
        return value;
    }

    @Override
    public String valueAsString(Fieldable field) {
        return value(field);
    }

    @Override
    public String indexedValue(String value) {
        return value;
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
        return fieldFilter(nullValue, null);
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
                        } else {
                            throw new ElasticSearchIllegalArgumentException("unknown property [" + currentFieldName + "]");
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
        if (!indexed() && !stored()) {
            context.ignoredValue(names.indexName(), value);
            return null;
        }
        Field field = new Field(names.indexName(), false, value, store, index, termVector);
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
        if (index != Defaults.INDEX) {
            builder.field("index", index.name().toLowerCase());
        }
        if (store != Defaults.STORE) {
            builder.field("store", store.name().toLowerCase());
        }
        if (termVector != Defaults.TERM_VECTOR) {
            builder.field("term_vector", termVector.name().toLowerCase());
        }
        if (omitNorms != Defaults.OMIT_NORMS) {
            builder.field("omit_norms", omitNorms);
        }
        if (indexOptions != Defaults.INDEX_OPTIONS) {
            builder.field("index_options", indexOptionToString(indexOptions));
        }
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
}
