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
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder for the {@link Filters} aggregation.
 */
public class FiltersAggregationBuilder extends AggregationBuilder<FiltersAggregationBuilder> {
    
    private Map<String, FilterBuilder> keyedFilters = null;
    private List<FilterBuilder> nonKeyedFilters = null;

    /**
     * Sole constructor.
     */
    public FiltersAggregationBuilder(String name) {
        super(name, InternalFilters.TYPE.name());
    }

    /**
     * Add a new filter with the given key.
     * NOTE: if a filter was already defined for this key, then this filter will replace it.
     * NOTE: the same {@link FiltersBuilder} cannot have both keyed and non-keyed filters
     */
    public FiltersAggregationBuilder filter(String key, FilterBuilder filter) {
        if (keyedFilters == null) {
            keyedFilters = new LinkedHashMap<>();
        }
        keyedFilters.put(key, filter);
        return this;
    }

    /**
     * Add a new filter with no key.
     * NOTE: the same {@link FiltersBuilder} cannot have both keyed and non-keyed filters.
     */
    public FiltersAggregationBuilder filter(FilterBuilder filter) {
        if (nonKeyedFilters == null) {
            nonKeyedFilters = new ArrayList<>();
        }
        nonKeyedFilters.add(filter);
        return this;
    }


    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (keyedFilters == null && nonKeyedFilters == null) {
            throw new SearchSourceBuilderException("At least one filter must be set on filter aggregation [" + getName() + "]");
        }
        if (keyedFilters != null && nonKeyedFilters != null) {
            throw new SearchSourceBuilderException("Cannot add both keyed and non-keyed filters to filters aggregation");
        }
        
        if (keyedFilters != null) {
            builder.startObject("filters");
            for (Map.Entry<String, FilterBuilder> entry : keyedFilters.entrySet()) {
                builder.field(entry.getKey());
                entry.getValue().toXContent(builder, params);
            }
            builder.endObject();
        }
        if (nonKeyedFilters != null) {
            builder.startArray("filters");
            for (FilterBuilder filterBuilder : nonKeyedFilters) {
                filterBuilder.toXContent(builder, params);
            }
            builder.endArray();

        }
        return builder.endObject();
    }
}
