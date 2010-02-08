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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.codehaus.jackson.JsonToken;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonStringFieldMapper extends JsonFieldMapper<String> {

    public static class Defaults extends JsonFieldMapper.Defaults {
        // NOTE, when adding defaults here, make sure you add them in the builder
        public static final String NULL_VALUE = null;
    }

    public static class Builder extends JsonFieldMapper.Builder<Builder, JsonStringFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name);
            builder = this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override public JsonStringFieldMapper build(BuilderContext context) {
            return new JsonStringFieldMapper(name, buildIndexName(context), buildFullName(context),
                    index, store, termVector, boost, omitNorms, omitTermFreqAndPositions, nullValue,
                    indexAnalyzer, searchAnalyzer);
        }
    }

    private final String nullValue;

    protected JsonStringFieldMapper(String name, String indexName, String fullName, Field.Index index, Field.Store store, Field.TermVector termVector,
                                    float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                    String nullValue, Analyzer indexAnalyzer, Analyzer searchAnalyzer) {
        super(name, indexName, fullName, index, store, termVector, boost, omitNorms, omitTermFreqAndPositions, indexAnalyzer, searchAnalyzer);
        this.nullValue = nullValue;
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
        if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
            value = nullValue;
        } else {
            value = jsonContext.jp().getText();
        }
        if (value == null) {
            return null;
        }
        return new Field(indexName, value, store, index, termVector);
    }
}
