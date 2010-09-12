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
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class XContentIdFieldMapper extends XContentFieldMapper<String> implements IdFieldMapper {

    public static final String CONTENT_TYPE = "_id";

    public static class Defaults extends XContentFieldMapper.Defaults {
        public static final String NAME = "_id";
        public static final String INDEX_NAME = "_id";
        public static final Field.Index INDEX = Field.Index.NOT_ANALYZED;
        public static final Field.Store STORE = Field.Store.NO;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
    }

    public static class Builder extends XContentFieldMapper.Builder<Builder, XContentIdFieldMapper> {

        public Builder() {
            super(Defaults.NAME);
            indexName = Defaults.INDEX_NAME;
            store = Defaults.STORE;
            index = Defaults.INDEX;
            omitNorms = Defaults.OMIT_NORMS;
            omitTermFreqAndPositions = Defaults.OMIT_TERM_FREQ_AND_POSITIONS;
        }

        @Override public XContentIdFieldMapper build(BuilderContext context) {
            return new XContentIdFieldMapper(name, indexName, store, termVector, boost, omitNorms, omitTermFreqAndPositions);
        }
    }

    protected XContentIdFieldMapper() {
        this(Defaults.NAME, Defaults.INDEX_NAME);
    }

    protected XContentIdFieldMapper(String name, String indexName) {
        this(name, indexName, Defaults.STORE, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS);
    }

    protected XContentIdFieldMapper(String name, String indexName, Field.Store store, Field.TermVector termVector,
                                    float boost, boolean omitNorms, boolean omitTermFreqAndPositions) {
        super(new Names(name, indexName, indexName, name), Defaults.INDEX, store, termVector, boost, omitNorms, omitTermFreqAndPositions,
                Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
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

    @Override protected Field parseCreateField(ParseContext context) throws IOException {
        if (context.parsedIdState() == ParseContext.ParsedIdState.NO) {
            String id = context.parser().text();
            if (context.id() != null && !context.id().equals(id)) {
                throw new MapperParsingException("Provided id [" + context.id() + "] does not match the content one [" + id + "]");
            }
            context.id(id);
            context.parsedId(ParseContext.ParsedIdState.PARSED);
            return new Field(names.indexName(), context.id(), store, index);
        } else if (context.parsedIdState() == ParseContext.ParsedIdState.EXTERNAL) {
            if (context.id() == null) {
                throw new MapperParsingException("No id mapping with [" + names.name() + "] found in the content, and not explicitly set");
            }
            return new Field(names.indexName(), context.id(), store, index);
        } else {
            throw new MapperParsingException("Illegal parsed id state");
        }
    }

    @Override protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(CONTENT_TYPE);
        builder.field("store", store.name().toLowerCase());
        builder.endObject();
    }

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }
}
