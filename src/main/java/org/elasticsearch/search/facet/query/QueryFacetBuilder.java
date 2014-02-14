/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.search.facet.query;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetBuilder;

import java.io.IOException;

/**
 *
 */
public class QueryFacetBuilder extends FacetBuilder {

    private QueryBuilder query;

    public QueryFacetBuilder(String name) {
        super(name);
    }

    /**
     * Marks the facet to run in a global scope, not bounded by any query.
     */
    @Override
    public QueryFacetBuilder global(boolean global) {
        super.global(global);
        return this;
    }

    public QueryFacetBuilder facetFilter(FilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    /**
     * Sets the nested path the facet will execute on. A match (root object) will then cause all the
     * nested objects matching the path to be computed into the facet.
     */
    public QueryFacetBuilder nested(String nested) {
        this.nested = nested;
        return this;
    }

    public QueryFacetBuilder query(QueryBuilder query) {
        this.query = query;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (query == null) {
            throw new SearchSourceBuilderException("query must be set on query facet for facet [" + name + "]");
        }
        builder.startObject(name);
        builder.field(QueryFacet.TYPE);
        query.toXContent(builder, params);

        addFilterFacetAndGlobal(builder, params);

        builder.endObject();
        return builder;
    }
}
