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
package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.support.QueryInnerHitBuilder;

import java.io.IOException;

/**
 * Builder for the 'has_parent' filter.
 */
public class HasParentFilterBuilder extends BaseFilterBuilder {

    private final QueryBuilder queryBuilder;
    private final FilterBuilder filterBuilder;
    private final String parentType;
    private String filterName;
    private QueryInnerHitBuilder innerHit = null;

    /**
     * @param parentType  The parent type
     * @param parentQuery The query that will be matched with parent documents
     */
    public HasParentFilterBuilder(String parentType, QueryBuilder parentQuery) {
        this.parentType = parentType;
        this.queryBuilder = parentQuery;
        this.filterBuilder = null;
    }

    /**
     * @param parentType   The parent type
     * @param parentFilter The filter that will be matched with parent documents
     */
    public HasParentFilterBuilder(String parentType, FilterBuilder parentFilter) {
        this.parentType = parentType;
        this.queryBuilder = null;
        this.filterBuilder = parentFilter;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public HasParentFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * This is a noop since has_parent can't be cached.
     */
    public HasParentFilterBuilder cache(boolean cache) {
        return this;
    }

    /**
     * This is a noop since has_parent can't be cached.
     */
    public HasParentFilterBuilder cacheKey(String cacheKey) {
        return this;
    }

    /**
     * Sets inner hit definition in the scope of this filter and reusing the defined type and query.
     */
    public HasParentFilterBuilder innerHit(QueryInnerHitBuilder innerHit) {
        this.innerHit = innerHit;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(HasParentFilterParser.NAME);
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        } else if (filterBuilder != null) {
            builder.field("filter");
            filterBuilder.toXContent(builder, params);
        }
        builder.field("parent_type", parentType);
        if (filterName != null) {
            builder.field("_name", filterName);
        }
        if (innerHit != null) {
            builder.startObject("inner_hits");
            builder.value(innerHit);
            builder.endObject();
        }
        builder.endObject();
    }
}

