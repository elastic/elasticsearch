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

package org.elasticsearch.search.facet.query;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.xcontent.XContentFilterBuilder;
import org.elasticsearch.index.query.xcontent.XContentQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.AbstractFacetBuilder;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class QueryFacetBuilder extends AbstractFacetBuilder {

    private XContentQueryBuilder query;

    public QueryFacetBuilder(String name) {
        super(name);
    }

    /**
     * Marks the facet to run in a global scope, not bounded by any query.
     */
    @Override public QueryFacetBuilder global(boolean global) {
        super.global(global);
        return this;
    }

    /**
     * Marks the facet to run in a specific scope.
     */
    @Override public QueryFacetBuilder scope(String scope) {
        super.scope(scope);
        return this;
    }

    public QueryFacetBuilder facetFilter(XContentFilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    public QueryFacetBuilder query(XContentQueryBuilder query) {
        this.query = query;
        return this;
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
