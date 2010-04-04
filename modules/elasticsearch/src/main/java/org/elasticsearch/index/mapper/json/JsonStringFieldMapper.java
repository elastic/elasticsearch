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

package org.elasticsearch.index.mapper.json;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.node.ObjectNode;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.index.mapper.json.JsonMapperBuilders.*;
import static org.elasticsearch.index.mapper.json.JsonTypeParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class JsonStringFieldMapper extends JsonFieldMapper<String> implements JsonIncludeInAllMapper {

    public static final String JSON_TYPE = "string";

    public static class Defaults extends JsonFieldMapper.Defaults {
        // NOTE, when adding defaults here, make sure you add them in the builder
        public static final String NULL_VALUE = null;
    }

    public static class Builder extends JsonFieldMapper.OpenBuilder<Builder, JsonStringFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name);
            builder = this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override public Builder includeInAll(Boolean includeInAll) {
            this.includeInAll = includeInAll;
            return this;
        }

        @Override public JsonStringFieldMapper build(BuilderContext context) {
            JsonStringFieldMapper fieldMapper = new JsonStringFieldMapper(buildNames(context),
                    index, store, termVector, boost, omitNorms, omitTermFreqAndPositions, nullValue,
                    indexAnalyzer, searchAnalyzer);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode stringNode = (ObjectNode) node;
            JsonStringFieldMapper.Builder builder = stringField(name);
            parseJsonField(builder, name, stringNode, parserContext);
            for (Iterator<Map.Entry<String, JsonNode>> propsIt = stringNode.getFields(); propsIt.hasNext();) {
                Map.Entry<String, JsonNode> entry = propsIt.next();
                String propName = entry.getKey();
                JsonNode propNode = entry.getValue();
                if (propName.equals("nullValue")) {
                    builder.nullValue(propNode.getValueAsText());
                }
            }
            return builder;
        }
    }


    private final String nullValue;

    private Boolean includeInAll;

    protected JsonStringFieldMapper(Names names, Field.Index index, Field.Store store, Field.TermVector termVector,
                                    float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                    String nullValue, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer) {
        super(names, index, store, termVector, boost, omitNorms, omitTermFreqAndPositions, indexAnalyzer, searchAnalyzer);
        this.nullValue = nullValue;
    }

    @Override public void includeInAll(Boolean includeInAll) {
        if (includeInAll != null) {
            this.includeInAll = includeInAll;
        }
    }

    @Override public String value(Fieldable field) {
        return field.stringValue();
    }

    @Override public String valueAsString(Fieldable field) {
        return value(field);
    }

    @Override public String indexedValue(String value) {
        return value;
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        String value;
        if (jsonContext.externalValueSet()) {
            value = (String) jsonContext.externalValue();
            if (value == null) {
                value = nullValue;
            }
        } else {
            if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
                value = nullValue;
            } else {
                value = jsonContext.jp().getText();
            }
        }
        if (value == null) {
            return null;
        }
        if (includeInAll == null || includeInAll) {
            jsonContext.allEntries().addText(names.fullName(), value, boost);
        }
        if (!indexed() && !stored()) {
            return null;
        }
        return new Field(names.indexName(), value, store, index, termVector);
    }

    @Override protected String jsonType() {
        return JSON_TYPE;
    }

    @Override protected void doJsonBody(JsonBuilder builder) throws IOException {
        super.doJsonBody(builder);
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        }
    }
}
