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

import java.io.IOException;

/**
 *
 */
public class HasChildFilterBuilder extends BaseFilterBuilder {

    private final FilterBuilder filterBuilder;
    private final QueryBuilder queryBuilder;
    private String childType;
    private String filterName;
    private Integer shortCircuitCutoff;

    public HasChildFilterBuilder(String type, QueryBuilder queryBuilder) {
        this.childType = type;
        this.queryBuilder = queryBuilder;
        this.filterBuilder = null;
    }

    public HasChildFilterBuilder(String type, FilterBuilder filterBuilder) {
        this.childType = type;
        this.queryBuilder = null;
        this.filterBuilder = filterBuilder;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public HasChildFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * This is a noop since has_child can't be cached.
     */
    public HasChildFilterBuilder cache(boolean cache) {
        return this;
    }

    /**
     * This is a noop since has_child can't be cached.
     */
    public HasChildFilterBuilder cacheKey(String cacheKey) {
        return this;
    }

    /**
     * Configures at what cut off point only to evaluate parent documents that contain the matching parent id terms
     * instead of evaluating all parent docs.
     */
    public HasChildFilterBuilder setShortCircuitCutoff(int shortCircuitCutoff) {
        this.shortCircuitCutoff = shortCircuitCutoff;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(HasChildFilterParser.NAME);
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        } else if (filterBuilder != null) {
            builder.field("filter");
            filterBuilder.toXContent(builder, params);
        }
        builder.field("child_type", childType);
        if (filterName != null) {
            builder.field("_name", filterName);
        }
        if (shortCircuitCutoff != null) {
            builder.field("short_circuit_cutoff", shortCircuitCutoff);
        }
        builder.endObject();
    }
}

