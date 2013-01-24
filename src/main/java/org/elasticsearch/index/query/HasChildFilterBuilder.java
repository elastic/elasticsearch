/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
    private String scope;
    private String filterName;
    private String executionType;

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

    public HasChildFilterBuilder scope(String scope) {
        this.scope = scope;
        return this;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public HasChildFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Expert: Sets the low level child to parent filtering implementation. Can be: 'bitset' or 'uid'
     * <p/>
     * This option is experimental and will be removed.
     */
    public HasChildFilterBuilder executionType(String executionType) {
        this.executionType = executionType;
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
        if (scope != null) {
            builder.field("_scope", scope);
        }
        if (filterName != null) {
            builder.field("_name", filterName);
        }
        if (executionType != null) {
            builder.field("execution_type", executionType);
        }
        builder.endObject();
    }
}

