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

import org.elasticsearch.index.query.json.JsonQueryBuilder;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SearchSourceFacetsBuilder {

    private String queryExecution;

    private List<FacetQuery> queryFacets;

    public SearchSourceFacetsBuilder queryExecution(String queryExecution) {
        this.queryExecution = queryExecution;
        return this;
    }

    public SearchSourceFacetsBuilder facet(String name, JsonQueryBuilder query) {
        if (queryFacets == null) {
            queryFacets = newArrayListWithCapacity(2);
        }
        queryFacets.add(new FacetQuery(name, query));
        return this;
    }

    void json(JsonBuilder builder) throws IOException {
        if (queryExecution == null && queryFacets == null) {
            return;
        }
        builder.field("facets");

        builder.startObject();

        if (queryExecution != null) {
            builder.field("queryExecution", queryExecution);
        }
        if (queryFacets != null) {
            for (FacetQuery facetQuery : queryFacets) {
                builder.startObject(facetQuery.name());
                builder.field("query");
                facetQuery.queryBuilder().toJson(builder);
                builder.endObject();
            }
        }

        builder.endObject();
    }

    private static class FacetQuery {
        private final String name;
        private final JsonQueryBuilder queryBuilder;

        private FacetQuery(String name, JsonQueryBuilder queryBuilder) {
            this.name = name;
            this.queryBuilder = queryBuilder;
        }

        public String name() {
            return name;
        }

        public JsonQueryBuilder queryBuilder() {
            return queryBuilder;
        }
    }
}
