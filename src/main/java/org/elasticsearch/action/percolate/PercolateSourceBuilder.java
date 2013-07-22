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
import java.util.Map;

/**
 */
public class PercolateSourceBuilder implements ToXContent {

    private DocBuilder docBuilder;
    private GetBuilder getBuilder;
    private QueryBuilder queryBuilder;
    private FilterBuilder filterBuilder;

    public DocBuilder percolateDocument() {
        if (docBuilder == null) {
            getBuilder = null;
            docBuilder = new DocBuilder();
        }
        return docBuilder;
    }

    public DocBuilder getDoc() {
        return docBuilder;
    }

    public void setDoc(DocBuilder docBuilder) {
        this.docBuilder = docBuilder;
    }

    public GetBuilder percolateGet() {
        if (getBuilder == null) {
            docBuilder = null;
            getBuilder = new GetBuilder();
        }
        return getBuilder;
    }

    public GetBuilder getGet() {
        return getBuilder;
    }

    public void setGet(GetBuilder getBuilder) {
        this.getBuilder = getBuilder;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public void setQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public FilterBuilder getFilterBuilder() {
        return filterBuilder;
    }

    public void setFilterBuilder(FilterBuilder filterBuilder) {
        this.filterBuilder = filterBuilder;
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
        if (getBuilder != null) {
            getBuilder.toXContent(builder, params);
        }
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        }
        if (filterBuilder != null) {
            builder.field("filter");
            filterBuilder.toXContent(builder, params);
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

    public static GetBuilder getBuilder(String index, String type, String id) {
        return new GetBuilder().setIndex(index).setType(type).setId(id);
    }

    public static class GetBuilder implements ToXContent {

        private String index;
        private String type;
        private String id;
        private Long version;
        private String routing;
        private String preference;

        public String getIndex() {
            return index;
        }

        public GetBuilder setIndex(String index) {
            this.index = index;
            return this;
        }

        public String getType() {
            return type;
        }

        public GetBuilder setType(String type) {
            this.type = type;
            return this;
        }

        public String getId() {
            return id;
        }

        public GetBuilder setId(String id) {
            this.id = id;
            return this;
        }

        public Long getVersion() {
            return version;
        }

        public GetBuilder setVersion(Long version) {
            this.version = version;
            return this;
        }

        public String getRouting() {
            return routing;
        }

        public GetBuilder setRouting(String routing) {
            this.routing = routing;
            return this;
        }

        public String getPreference() {
            return preference;
        }

        public GetBuilder setPreference(String preference) {
            this.preference = preference;
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("get");
            builder.field("index", index);
            builder.field("type", type);
            builder.field("id", id);
            if (version != null) {
                builder.field("version", version);
            }
            if (routing != null) {
                builder.field("routing", routing);
            }
            builder.endObject();
            return builder;
        }

    }

}
