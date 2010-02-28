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
import org.apache.lucene.index.Term;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.lucene.Lucene;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonUidFieldMapper extends JsonFieldMapper<Uid> implements UidFieldMapper {

    public static final String JSON_TYPE = "uidField";

    public static class Defaults extends JsonFieldMapper.Defaults {
        public static final String NAME = UidFieldMapper.NAME;
        public static final Field.Index INDEX = Field.Index.NOT_ANALYZED;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
    }

    public static class Builder extends JsonMapper.Builder<Builder, JsonUidFieldMapper> {

        protected String indexName;

        public Builder() {
            super(Defaults.NAME);
            this.indexName = name;
        }

        @Override public JsonUidFieldMapper build(BuilderContext context) {
            return new JsonUidFieldMapper(name, indexName);
        }
    }

    protected JsonUidFieldMapper() {
        this(Defaults.NAME);
    }

    protected JsonUidFieldMapper(String name) {
        this(name, name);
    }

    protected JsonUidFieldMapper(String name, String indexName) {
        super(new Names(name, indexName, indexName, name), Defaults.INDEX, Field.Store.YES, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        if (jsonContext.id() == null) {
            throw new MapperParsingException("No id found while parsing the json source");
        }
        jsonContext.uid(Uid.createUid(jsonContext.stringBuilder(), jsonContext.type(), jsonContext.id()));
        return new Field(names.indexName(), jsonContext.uid(), store, index);
    }

    @Override public Uid value(Fieldable field) {
        return Uid.createUid(field.stringValue());
    }

    @Override public String valueAsString(Fieldable field) {
        return field.stringValue();
    }

    @Override public String indexedValue(String value) {
        return value;
    }

    @Override public Term term(String type, String id) {
        return term(Uid.createUid(type, id));
    }

    @Override public Term term(String uid) {
        return new Term(names.indexName(), uid);
    }

    @Override protected String jsonType() {
        return JSON_TYPE;
    }

    @Override public void toJson(JsonBuilder builder, Params params) throws IOException {
        // for now, don't output it at all
    }

    @Override public void merge(JsonMapper mergeWith, JsonMergeContext mergeContext) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }
}
