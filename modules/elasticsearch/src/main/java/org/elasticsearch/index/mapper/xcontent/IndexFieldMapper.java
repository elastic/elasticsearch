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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MergeMappingException;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class IndexFieldMapper extends AbstractFieldMapper<String> implements org.elasticsearch.index.mapper.IndexFieldMapper {

    public static final String CONTENT_TYPE = "_index";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = org.elasticsearch.index.mapper.IndexFieldMapper.NAME;
        public static final String INDEX_NAME = org.elasticsearch.index.mapper.IndexFieldMapper.NAME;
        public static final Field.Index INDEX = Field.Index.NOT_ANALYZED;
        public static final Field.Store STORE = Field.Store.NO;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
        public static final boolean ENABLED = false;
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, IndexFieldMapper> {

        private boolean enabled = Defaults.ENABLED;

        public Builder() {
            super(Defaults.NAME);
            indexName = Defaults.INDEX_NAME;
            index = Defaults.INDEX;
            store = Defaults.STORE;
            omitNorms = Defaults.OMIT_NORMS;
            omitTermFreqAndPositions = Defaults.OMIT_TERM_FREQ_AND_POSITIONS;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        @Override public IndexFieldMapper build(BuilderContext context) {
            return new IndexFieldMapper(name, indexName, store, termVector, boost, omitNorms, omitTermFreqAndPositions, enabled);
        }
    }

    private final boolean enabled;

    protected IndexFieldMapper() {
        this(Defaults.NAME, Defaults.INDEX_NAME);
    }

    protected IndexFieldMapper(String name, String indexName) {
        this(name, indexName, Defaults.STORE, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Defaults.ENABLED);
    }

    public IndexFieldMapper(String name, String indexName, Field.Store store, Field.TermVector termVector,
                            float boost, boolean omitNorms, boolean omitTermFreqAndPositions, boolean enabled) {
        super(new Names(name, indexName, indexName, name), Defaults.INDEX, store, termVector, boost, omitNorms, omitTermFreqAndPositions,
                Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
        this.enabled = enabled;
    }

    @Override public boolean enabled() {
        return this.enabled;
    }

    @Override public String value(Document document) {
        Fieldable field = document.getFieldable(names.indexName());
        return field == null ? null : value(field);
    }

    @Override public String value(Fieldable field) {
        return field.stringValue();
    }

    @Override public String valueFromString(String value) {
        return value;
    }

    @Override public String valueAsString(Fieldable field) {
        return value(field);
    }

    @Override public String indexedValue(String value) {
        return value;
    }

    @Override public Term term(String value) {
        return new Term(names.indexName(), value);
    }

    @Override protected Field parseCreateField(ParseContext context) throws IOException {
        if (!enabled) {
            return null;
        }
        return new Field(names.indexName(), context.index(), store, index);
    }

    @Override protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // if all defaults, no need to write it at all
        if (store == Defaults.STORE && enabled == Defaults.ENABLED) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (store != Defaults.STORE) {
            builder.field("store", store.name().toLowerCase());
        }
        if (enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }
        builder.endObject();
        return builder;
    }

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }
}
