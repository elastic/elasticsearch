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

package org.elasticsearch.search.aggregations.bucket.filters;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class FiltersAggregationBuilder extends AggregationBuilder<FiltersAggregationBuilder> {

    private Map<String, FilterBuilder> filters = new HashMap<>();

    public FiltersAggregationBuilder(String name) {
        super(name, InternalFilters.TYPE.name());
    }

    public FiltersAggregationBuilder filter(String key, FilterBuilder filter) {
        filters.put(key, filter);
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("filters");
        for (Map.Entry<String, FilterBuilder> entry : filters.entrySet()) {
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }
        builder.endObject();
        return builder.endObject();
    }
}
