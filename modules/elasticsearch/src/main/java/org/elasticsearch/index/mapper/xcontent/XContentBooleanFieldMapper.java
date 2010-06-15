/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.mapper.xcontent;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.Booleans;
import org.elasticsearch.util.Strings;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*;
import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.elasticsearch.index.mapper.xcontent.XContentTypeParsers.*;

/**
 * @author kimchy (shay.banon)
 */
// TODO this can be made better, maybe storing a byte for it?
public class XContentBooleanFieldMapper extends XContentFieldMapper<Boolean> {

    public static final String CONTENT_TYPE = "boolean";

    public static class Defaults extends XContentFieldMapper.Defaults {
        public static final boolean OMIT_NORMS = true;
        public static final Boolean NULL_VALUE = null;
    }

    public static class Builder extends XContentFieldMapper.Builder<Builder, XContentBooleanFieldMapper> {

        private Boolean nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name);
            this.omitNorms = Defaults.OMIT_NORMS;
            this.builder = this;
        }

        public Builder nullValue(boolean nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override public Builder index(Field.Index index) {
            return super.index(index);
        }

        @Override public Builder store(Field.Store store) {
            return super.store(store);
        }

        @Override public Builder termVector(Field.TermVector termVector) {
            return super.termVector(termVector);
        }

        @Override public Builder boost(float boost) {
            return super.boost(boost);
        }

        @Override public Builder indexName(String indexName) {
            return super.indexName(indexName);
        }

        @Override public Builder omitTermFreqAndPositions(boolean omitTermFreqAndPositions) {
            return super.omitTermFreqAndPositions(omitTermFreqAndPositions);
        }

        @Override public XContentBooleanFieldMapper build(BuilderContext context) {
            return new XContentBooleanFieldMapper(buildNames(context), index, store,
                    termVector, boost, omitNorms, omitTermFreqAndPositions, nullValue);
        }
    }

    public static class TypeParser implements XContentTypeParser {
        @Override public XContentMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            XContentBooleanFieldMapper.Builder builder = booleanField(name);
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

    protected XContentBooleanFieldMapper(Names names, Field.Index index, Field.Store store, Field.TermVector termVector,
                                         float boost, boolean omitNorms, boolean omitTermFreqAndPositions, Boolean nullValue) {
        super(names, index, store, termVector, boost, omitNorms, omitTermFreqAndPositions, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
        this.nullValue = nullValue;
    }

    @Override public Boolean value(Fieldable field) {
        return field.stringValue().charAt(0) == 'T' ? Boolean.TRUE : Boolean.FALSE;
    }

    @Override public String valueAsString(Fieldable field) {
        return field.stringValue().charAt(0) == 'T' ? "true" : "false";
    }

    @Override public String indexedValue(String value) {
        if (value == null || value.length() == 0) {
            return "F";
        }
        if (Booleans.parseBoolean(value, false)) {
            return "T";
        }
        return "F";
    }

    @Override public String indexedValue(Boolean value) {
        if (value == null) {
            return "F";
        }
        return value ? "T" : "F";
    }

    @Override protected Field parseCreateField(ParseContext context) throws IOException {
        if (!indexed() && !stored()) {
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
        return new Field(names.indexName(), value, store, index, termVector);
    }

    @Override protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override protected void doXContentBody(XContentBuilder builder) throws IOException {
        super.doXContentBody(builder);
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
    }
}
