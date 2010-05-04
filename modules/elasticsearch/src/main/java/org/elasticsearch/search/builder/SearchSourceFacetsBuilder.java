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

    private String queryExecution;

    private List<FacetQuery> queryFacets;

    /**
     * Controls the type of query facet execution.
     */
    public SearchSourceFacetsBuilder queryExecution(String queryExecution) {
        this.queryExecution = queryExecution;
        return this;
    }

    /**
     * Adds a query facet (which results in a count facet returned).
     *
     * @param name  The logical name of the facet, it will be returned under the name
     * @param query The query facet
     */
    public SearchSourceFacetsBuilder facet(String name, XContentQueryBuilder query) {
        if (queryFacets == null) {
            queryFacets = newArrayListWithCapacity(2);
        }
        queryFacets.add(new FacetQuery(name, query, null));
        return this;
    }

    /**
     * Adds a query facet (which results in a count facet returned) with an option to
     * be global on the index or bounded by the search query.
     *
     * @param name  The logical name of the facet, it will be returned under the name
     * @param query The query facet
     */
    public SearchSourceFacetsBuilder facet(String name, XContentQueryBuilder query, boolean global) {
        if (queryFacets == null) {
            queryFacets = newArrayListWithCapacity(2);
        }
        queryFacets.add(new FacetQuery(name, query, global));
        return this;
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        if (queryExecution == null && queryFacets == null) {
            return;
        }
        builder.field("facets");

        builder.startObject();

        if (queryExecution != null) {
            builder.field("query_execution", queryExecution);
        }
        if (queryFacets != null) {
            for (FacetQuery facetQuery : queryFacets) {
                builder.startObject(facetQuery.name());
                builder.field("query");
                facetQuery.queryBuilder().toXContent(builder, params);
                if (facetQuery.global() != null) {
                    builder.field("global", facetQuery.global());
                }
                builder.endObject();
            }
        }

        builder.endObject();
    }

    private static class FacetQuery {
        private final String name;
        private final XContentQueryBuilder queryBuilder;
        private final Boolean global;

        private FacetQuery(String name, XContentQueryBuilder queryBuilder, Boolean global) {
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
