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

package org.elasticsearch.search.builder;

import org.elasticsearch.index.query.xcontent.XContentQueryBuilder;
import org.elasticsearch.search.facets.query.QueryFacetCollectorParser;
import org.elasticsearch.search.facets.terms.TermFacetCollectorParser;
import org.elasticsearch.util.xcontent.ToXContent;
import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.util.collect.Lists.*;

/**
 * A search source facets builder.
 *
 * @author kimchy (shay.banon)
 * @see SearchSourceBuilder#facets(SearchSourceFacetsBuilder)
 */
public class SearchSourceFacetsBuilder implements ToXContent {

    private List<QueryFacet> queryFacets;
    private List<TermsFacet> termsFacets;

    /**
     * Adds a query facet (which results in a count facet returned).
     *
     * @param name  The logical name of the facet, it will be returned under the name
     * @param query The query facet
     */
    public SearchSourceFacetsBuilder queryFacet(String name, XContentQueryBuilder query) {
        if (queryFacets == null) {
            queryFacets = newArrayListWithCapacity(2);
        }
        queryFacets.add(new QueryFacet(name, query, false));
        return this;
    }

    /**
     * Adds a query facet (which results in a count facet returned) with an option to
     * be global on the index or bounded by the search query.
     *
     * @param name   The logical name of the facet, it will be returned under the name
     * @param query  The query facet
     * @param global Should the facet be executed globally or not
     */
    public SearchSourceFacetsBuilder queryFacetGlobal(String name, XContentQueryBuilder query) {
        if (queryFacets == null) {
            queryFacets = newArrayListWithCapacity(2);
        }
        queryFacets.add(new QueryFacet(name, query, true));
        return this;
    }

    public SearchSourceFacetsBuilder termsFacet(String name, String fieldName, int size) {
        if (termsFacets == null) {
            termsFacets = newArrayListWithCapacity(2);
        }
        termsFacets.add(new TermsFacet(name, fieldName, size, false));
        return this;
    }

    public SearchSourceFacetsBuilder termsFacetGlobal(String name, String fieldName, int size) {
        if (termsFacets == null) {
            termsFacets = newArrayListWithCapacity(2);
        }
        termsFacets.add(new TermsFacet(name, fieldName, size, true));
        return this;
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        if (queryFacets == null && termsFacets == null) {
            return;
        }
        builder.field("facets");

        builder.startObject();

        if (queryFacets != null) {
            for (QueryFacet queryFacet : queryFacets) {
                builder.startObject(queryFacet.name());
                builder.field(QueryFacetCollectorParser.NAME);
                queryFacet.queryBuilder().toXContent(builder, params);
                if (queryFacet.global() != null) {
                    builder.field("global", queryFacet.global());
                }
                builder.endObject();
            }
        }
        if (termsFacets != null) {
            for (TermsFacet termsFacet : termsFacets) {
                builder.startObject(termsFacet.name());

                builder.startObject(TermFacetCollectorParser.NAME);
                builder.field("field", termsFacet.fieldName());
                builder.field("size", termsFacet.size());
                builder.endObject();

                if (termsFacet.global() != null) {
                    builder.field("global", termsFacet.global());
                }

                builder.endObject();
            }
        }

        builder.endObject();
    }

    private static class TermsFacet {
        private final String name;
        private final String fieldName;
        private final int size;
        private final Boolean global;

        private TermsFacet(String name, String fieldName, int size, Boolean global) {
            this.name = name;
            this.fieldName = fieldName;
            this.size = size;
            this.global = global;
        }

        public String name() {
            return name;
        }

        public String fieldName() {
            return fieldName;
        }

        public int size() {
            return size;
        }

        public Boolean global() {
            return global;
        }
    }

    private static class QueryFacet {
        private final String name;
        private final XContentQueryBuilder queryBuilder;
        private final Boolean global;

        private QueryFacet(String name, XContentQueryBuilder queryBuilder, Boolean global) {
            this.name = name;
            this.queryBuilder = queryBuilder;
            this.global = global;
        }

        public String name() {
            return name;
        }

        public XContentQueryBuilder queryBuilder() {
            return queryBuilder;
        }

        public Boolean global() {
            return this.global;
        }
    }
}
