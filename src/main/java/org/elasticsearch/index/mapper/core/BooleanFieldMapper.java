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
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
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
        public Builder index(boolean index) {
            return super.index(index);
        }

        @Override
        public Builder store(boolean store) {
            return super.store(store);
        }

        @Override
        protected Builder storeTermVectors(boolean termVectors) {
            return super.storeTermVectors(termVectors);
        }

        @Override
        protected Builder storeTermVectorOffsets(boolean termVectorOffsets) {
            return super.storeTermVectorOffsets(termVectorOffsets);
        }

        @Override
        protected Builder storeTermVectorPositions(boolean termVectorPositions) {
            return super.storeTermVectorPositions(termVectorPositions);
        }

        @Override
        protected Builder storeTermVectorPayloads(boolean termVectorPayloads) {
            return super.storeTermVectorPayloads(termVectorPayloads);
        }

        @Override
        public Builder boost(float boost) {
            return super.boost(boost);
        }

        @Override
        public Builder indexName(String indexName) {
            return super.indexName(indexName);
        }

        @Override
        public Builder similarity(SimilarityProvider similarity) {
            return super.similarity(similarity);
        }

        @Override
        public BooleanFieldMapper build(BuilderContext context) {
            return new BooleanFieldMapper(buildNames(context), boost, fieldType, nullValue, provider, similarity);
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

    protected BooleanFieldMapper(Names names, float boost, FieldType fieldType, Boolean nullValue, PostingsFormatProvider provider, SimilarityProvider similarity) {
        super(names, boost, fieldType, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER, provider, similarity);
        this.nullValue = nullValue;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public boolean useFieldQueryWithQueryString() {
        return true;
    }

    @Override
    public Boolean value(Object value) {
        return valueFromString((String) value);
    }

    @Override
    public Boolean valueFromString(String value) {
        return value.charAt(0) == 'T' ? Boolean.TRUE : Boolean.FALSE;
    }

    @Override
    public String valueAsString(Object value) {
        return ((String) value).charAt(0) == 'T' ? "true" : "false";
    }

    @Override
    public BytesRef indexedValue(String value) {
        if (value == null || value.length() == 0) {
            return Values.FALSE;
        }
        if (value.length() == 1 && value.charAt(0) == 'F') {
            return Values.FALSE;
        }
        if (Booleans.parseBoolean(value, false)) {
            return Values.TRUE;
        }
        return Values.FALSE;
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        return new TermFilter(names().createIndexNameTerm(nullValue ? "T" : "F"));
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
    protected void doXContentBody(XContentBuilder builder) throws IOException {
        super.doXContentBody(builder);
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
    }
}
