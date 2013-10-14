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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.MapperBuilders.booleanField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
// TODO this can be made better, maybe storing a byte for it?
public class BooleanFieldMapper extends AbstractFieldMapper<Boolean> {

    public static final String CONTENT_TYPE = "boolean";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.freeze();
        }

        public static final Boolean NULL_VALUE = null;
    }

    public static class Values {
        public final static BytesRef TRUE = new BytesRef("T");
        public final static BytesRef FALSE = new BytesRef("F");
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, BooleanFieldMapper> {

        private Boolean nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
            this.builder = this;
        }

        public Builder nullValue(boolean nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        protected Builder tokenized(boolean tokenized) {
            if (tokenized) {
                throw new ElasticSearchIllegalArgumentException("bool field can't be tokenized");
            }
            return super.tokenized(tokenized);
        }

        @Override
        public BooleanFieldMapper build(BuilderContext context) {
            return new BooleanFieldMapper(buildNames(context), boost, fieldType, nullValue, provider, similarity, fieldDataSettings);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            BooleanFieldMapper.Builder builder = booleanField(name);
            parseField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    builder.nullValue(nodeBooleanValue(propNode));
                }
            }
            return builder;
        }
    }

    private Boolean nullValue;

    protected BooleanFieldMapper(Names names, float boost, FieldType fieldType, Boolean nullValue, PostingsFormatProvider provider, SimilarityProvider similarity, @Nullable Settings fieldDataSettings) {
        super(names, boost, fieldType, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER, provider, similarity, fieldDataSettings);
        this.nullValue = nullValue;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        // TODO have a special boolean type?
        return new FieldDataType("string");
    }

    @Override
    public boolean useTermQueryWithQueryString() {
        return true;
    }

    @Override
    public Boolean value(Object value) {
        if (value == null) {
            return Boolean.FALSE;
        }
        String sValue = value.toString();
        if (sValue.length() == 0) {
            return Boolean.FALSE;
        }
        if (sValue.length() == 1 && sValue.charAt(0) == 'F') {
            return Boolean.FALSE;
        }
        if (Booleans.parseBoolean(sValue, false)) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    @Override
    public Object valueForSearch(Object value) {
        return value(value);
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        if (value == null) {
            return Values.FALSE;
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? Values.TRUE : Values.FALSE;
        }
        String sValue;
        if (value instanceof BytesRef) {
            sValue = ((BytesRef) value).utf8ToString();
        } else {
            sValue = value.toString();
        }
        if (sValue.length() == 0) {
            return Values.FALSE;
        }
        if (sValue.length() == 1 && sValue.charAt(0) == 'F') {
            return Values.FALSE;
        }
        if (Booleans.parseBoolean(sValue, false)) {
            return Values.TRUE;
        }
        return Values.FALSE;
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        return new TermFilter(names().createIndexNameTerm(nullValue ? Values.TRUE : Values.FALSE));
    }

    @Override
    protected Field parseCreateField(ParseContext context) throws IOException {
        if (!fieldType().indexed() && !fieldType().stored()) {
            return null;
        }
        XContentParser.Token token = context.parser().currentToken();
        String value = null;
        if (token == XContentParser.Token.VALUE_NULL) {
            if (nullValue != null) {
                value = nullValue ? "T" : "F";
            }
        } else {
            value = context.parser().booleanValue() ? "T" : "F";
        }
        if (value == null) {
            return null;
        }
        return new Field(names.indexName(), value, fieldType);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || nullValue != null) {
            builder.field("null_value", nullValue);
        }
    }
}
