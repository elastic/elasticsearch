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

package org.elasticsearch.search.aggregations.bucket.exclude;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class ExcludeBuilder extends AggregationBuilder<ExcludeBuilder> {

    private Boolean excludeQuery;

    private List<String> excludeFilters;

    public ExcludeBuilder(String name) {
        super(name, InternalExclude.TYPE.name());
    }

    public ExcludeBuilder excludeQuery(Boolean excludeQuery) {
        this.excludeQuery = excludeQuery;
        return this;
    }

    public ExcludeBuilder excludeFilters(List<String> excludeFilters) {
        this.excludeFilters = excludeFilters;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if(excludeFilters != null) {
            builder.startArray("exclude_filters");
            for (String filter : excludeFilters) {
                builder.value(filter);
            }
            builder.endArray();
        }

        if (excludeQuery != null) {
            builder.field("exclude_query", excludeQuery);
        }
        builder.endObject();

        return builder;
    }

}
