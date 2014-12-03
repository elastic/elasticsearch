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

public class NestedFilterBuilder extends BaseFilterBuilder {

    private final QueryBuilder queryBuilder;
    private final FilterBuilder filterBuilder;

    private final String path;
    private Boolean join;

    private Boolean cache;
    private String cacheKey;
    private String filterName;

    private QueryInnerHitBuilder innerHit = null;

    public NestedFilterBuilder(String path, QueryBuilder queryBuilder) {
        this.path = path;
        this.queryBuilder = queryBuilder;
        this.filterBuilder = null;
    }

    public NestedFilterBuilder(String path, FilterBuilder filterBuilder) {
        this.path = path;
        this.queryBuilder = null;
        this.filterBuilder = filterBuilder;
    }

    public NestedFilterBuilder join(boolean join) {
        this.join = join;
        return this;
    }

    /**
     * Should the filter be cached or not. Defaults to <tt>false</tt>.
     */
    public NestedFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public NestedFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public NestedFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Sets inner hit definition in the scope of this nested filter and reusing the defined path and query.
     */
    public NestedFilterBuilder innerHit(QueryInnerHitBuilder innerHit) {
        this.innerHit = innerHit;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NestedFilterParser.NAME);
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        } else {
            builder.field("filter");
            filterBuilder.toXContent(builder, params);
        }
        if (join != null) {
            builder.field("join", join);
        }
        builder.field("path", path);
        if (filterName != null) {
            builder.field("_name", filterName);
        }
        if (cache != null) {
            builder.field("_cache", cache);
        }
        if (cacheKey != null) {
            builder.field("_cache_key", cacheKey);
        }
        if (innerHit != null) {
            builder.startObject("inner_hits");
            builder.value(innerHit);
            builder.endObject();
        }
        builder.endObject();
    }
}