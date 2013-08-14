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

package org.elasticsearch.action.percolate;

import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class PercolateSourceBuilder implements ToXContent {

    private DocBuilder docBuilder;
    private QueryBuilder queryBuilder;
    private FilterBuilder filterBuilder;
    private Integer size;
    private Boolean sort;
    private Boolean score;

    public DocBuilder percolateDocument() {
        if (docBuilder == null) {
            docBuilder = new DocBuilder();
        }
        return docBuilder;
    }

    public DocBuilder getDoc() {
        return docBuilder;
    }

    public PercolateSourceBuilder setDoc(DocBuilder docBuilder) {
        this.docBuilder = docBuilder;
        return this;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public PercolateSourceBuilder setQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        return this;
    }

    public FilterBuilder getFilterBuilder() {
        return filterBuilder;
    }

    public PercolateSourceBuilder setFilterBuilder(FilterBuilder filterBuilder) {
        this.filterBuilder = filterBuilder;
        return this;
    }

    public PercolateSourceBuilder setSize(int size) {
        this.size = size;
        return this;
    }

    public PercolateSourceBuilder setSort(boolean sort) {
        this.sort = sort;
        return this;
    }

    public PercolateSourceBuilder setScore(boolean score) {
        this.score = score;
        return this;
    }

    public BytesReference buildAsBytes(XContentType contentType) throws SearchSourceBuilderException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.bytes();
        } catch (Exception e) {
            throw new SearchSourceBuilderException("Failed to build search source", e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (docBuilder != null) {
            docBuilder.toXContent(builder, params);
        }
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        }
        if (filterBuilder != null) {
            builder.field("filter");
            filterBuilder.toXContent(builder, params);
        }
        if (size != null) {
            builder.field("size", size);
        }
        if (sort != null) {
            builder.field("sort", sort);
        }
        if (score != null) {
            builder.field("score", score);
        }
        builder.endObject();
        return builder;
    }

    public static DocBuilder docBuilder() {
        return new DocBuilder();
    }

    public static class DocBuilder implements ToXContent {

        private BytesReference doc;

        public DocBuilder setDoc(BytesReference doc) {
            this.doc = doc;
            return this;
        }

        public DocBuilder setDoc(String field, Object value) {
            Map<String, Object> values = new HashMap<String, Object>(2);
            values.put(field, value);
            setDoc(values);
            return this;
        }

        public DocBuilder setDoc(String doc) {
            this.doc = new BytesArray(doc);
            return this;
        }

        public DocBuilder setDoc(XContentBuilder doc) {
            this.doc = doc.bytes();
            return this;
        }

        public DocBuilder setDoc(Map doc) {
            return setDoc(doc, PercolateRequest.contentType);
        }

        public DocBuilder setDoc(Map doc, XContentType contentType) {
            try {
                return setDoc(XContentFactory.contentBuilder(contentType).map(doc));
            } catch (IOException e) {
                throw new ElasticSearchGenerationException("Failed to generate [" + doc + "]", e);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            XContentType contentType = XContentFactory.xContentType(doc);
            if (contentType == builder.contentType()) {
                builder.rawField("doc", doc);
            } else {
                XContentParser parser = XContentFactory.xContent(contentType).createParser(doc);
                try {
                    parser.nextToken();
                    builder.field("doc");
                    builder.copyCurrentStructure(parser);
                } finally {
                    parser.close();
                }
            }
            return builder;
        }
    }

}
