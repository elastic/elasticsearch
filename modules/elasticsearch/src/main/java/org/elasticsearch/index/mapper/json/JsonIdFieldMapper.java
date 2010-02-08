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
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.lucene.Lucene;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonIdFieldMapper extends JsonFieldMapper<String> implements IdFieldMapper {

    public static class Defaults extends JsonFieldMapper.Defaults {
        public static final String NAME = "_id";
        public static final String INDEX_NAME = "_id";
        public static final Field.Index INDEX = Field.Index.NOT_ANALYZED;
        public static final Field.Store STORE = Field.Store.NO;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
    }

    public static class Builder extends JsonFieldMapper.Builder<Builder, JsonIdFieldMapper> {

        public Builder(String name) {
            super(name);
            indexName = Defaults.INDEX_NAME;
            store = Defaults.STORE;
            index = Defaults.INDEX;
            omitNorms = Defaults.OMIT_NORMS;
            omitTermFreqAndPositions = Defaults.OMIT_TERM_FREQ_AND_POSITIONS;
        }

        @Override public JsonIdFieldMapper build(BuilderContext context) {
            return new JsonIdFieldMapper(name, indexName, store, termVector, boost, omitNorms, omitTermFreqAndPositions);
        }
    }

    protected JsonIdFieldMapper() {
        this(Defaults.NAME, Defaults.INDEX_NAME);
    }

    protected JsonIdFieldMapper(String name, String indexName) {
        this(name, indexName, Defaults.STORE, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS);
    }

    public JsonIdFieldMapper(String name, String indexName, Field.Store store, Field.TermVector termVector,
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

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        if (jsonContext.parsedIdState() == JsonParseContext.ParsedIdState.NO) {
            String id = jsonContext.jp().getText();
            if (jsonContext.id() != null && !jsonContext.id().equals(id)) {
                throw new MapperParsingException("Provided id [" + jsonContext.id() + "] does not match the json one [" + id + "]");
            }
            jsonContext.id(id);
            jsonContext.parsedId(JsonParseContext.ParsedIdState.PARSED);
            return new Field(indexName, jsonContext.id(), store, index);
        } else if (jsonContext.parsedIdState() == JsonParseContext.ParsedIdState.EXTERNAL) {
            if (jsonContext.id() == null) {
                throw new MapperParsingException("No id mapping with [" + name() + "] found in the json, and not explicitly set");
            }
            return new Field(indexName, jsonContext.id(), store, index);
        } else {
            throw new MapperParsingException("Illegal parsed id state");
        }
    }

    @Override public void traverse(FieldMapperListener fieldMapperListener) {
        fieldMapperListener.fieldMapper(this);
    }
}
