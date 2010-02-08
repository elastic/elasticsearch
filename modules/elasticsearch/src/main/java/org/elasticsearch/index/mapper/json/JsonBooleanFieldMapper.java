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
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.util.lucene.Lucene;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
// TODO this can be made better, maybe storing a byte for it?
public class JsonBooleanFieldMapper extends JsonFieldMapper<Boolean> {

    public static class Defaults extends JsonFieldMapper.Defaults {
        public static final boolean OMIT_NORMS = true;
        public static final Boolean NULL_VALUE = null;
    }

    public static class Builder extends JsonFieldMapper.Builder<Builder, JsonBooleanFieldMapper> {

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

        @Override public JsonBooleanFieldMapper build(BuilderContext context) {
            return new JsonBooleanFieldMapper(name, buildIndexName(context), buildFullName(context), index, store,
                    termVector, boost, omitNorms, omitTermFreqAndPositions, nullValue);
        }
    }

    private Boolean nullValue;

    protected JsonBooleanFieldMapper(String name, String indexName, String fullName, Field.Index index, Field.Store store, Field.TermVector termVector,
                                     float boost, boolean omitNorms, boolean omitTermFreqAndPositions, Boolean nullValue) {
        super(name, indexName, fullName, index, store, termVector, boost, omitNorms, omitTermFreqAndPositions,
                Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
        this.nullValue = nullValue;
    }

    @Override public Boolean value(Fieldable field) {
        return Boolean.parseBoolean(valueAsString(field));
    }

    @Override public String valueAsString(Fieldable field) {
        return field.stringValue().charAt(0) == 'T' ? "true" : "false";
    }

    @Override public String indexedValue(String value) {
        if (value == null || value.length() == 0) {
            return "F";
        }
        return value.equals("true") ? "T" : "F";
    }

    @Override public String indexedValue(Boolean value) {
        if (value == null) {
            return "F";
        }
        return value ? "T" : "F";
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        JsonToken token = jsonContext.jp().getCurrentToken();
        String value = null;
        if (token == JsonToken.VALUE_FALSE) {
            value = "F";
        } else if (token == JsonToken.VALUE_TRUE) {
            value = "T";
        } else if (token == JsonToken.VALUE_NULL) {
            if (nullValue != null) {
                value = nullValue ? "T" : "F";
            }
        } else {
            return null;
        }
        if (value == null) {
            return null;
        }
        return new Field(indexName, value, store, index, termVector);
    }
}
