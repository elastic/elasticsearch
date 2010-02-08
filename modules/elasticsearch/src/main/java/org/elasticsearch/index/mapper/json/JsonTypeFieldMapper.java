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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.util.lucene.Lucene;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonTypeFieldMapper extends JsonFieldMapper<String> implements TypeFieldMapper {

    public static class Defaults extends JsonFieldMapper.Defaults {
        public static final String NAME = "_type";
        public static final String INDEX_NAME = "_type";
        public static final Field.Index INDEX = Field.Index.NOT_ANALYZED;
        public static final Field.Store STORE = Field.Store.NO;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
    }

    public static class Builder extends JsonFieldMapper.Builder<Builder, JsonTypeFieldMapper> {

        public Builder(String name) {
            super(name);
            indexName = Defaults.INDEX_NAME;
            index = Defaults.INDEX;
            store = Defaults.STORE;
            omitNorms = Defaults.OMIT_NORMS;
            omitTermFreqAndPositions = Defaults.OMIT_TERM_FREQ_AND_POSITIONS;
        }

        @Override public JsonTypeFieldMapper build(BuilderContext context) {
            return new JsonTypeFieldMapper(name, indexName, store, termVector, boost, omitNorms, omitTermFreqAndPositions);
        }
    }

    protected JsonTypeFieldMapper() {
        this(Defaults.NAME, Defaults.INDEX_NAME);
    }

    protected JsonTypeFieldMapper(String name, String indexName) {
        this(name, indexName, Defaults.STORE, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS);
    }

    public JsonTypeFieldMapper(String name, String indexName, Field.Store store, Field.TermVector termVector,
                               float boost, boolean omitNorms, boolean omitTermFreqAndPositions) {
        super(name, indexName, name, Defaults.INDEX, store, termVector, boost, omitNorms, omitTermFreqAndPositions,
                Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
    }

    @Override public String value(Document document) {
        Fieldable field = document.getFieldable(indexName);
        return field == null ? null : value(field);
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

    @Override public Term term(String value) {
        return new Term(indexName, value);
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        return new Field(indexName, jsonContext.type(), store, index);
    }
}
