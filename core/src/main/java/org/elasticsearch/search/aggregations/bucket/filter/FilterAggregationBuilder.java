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

package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;

/**
 * Builder for the {@link Filter} aggregation.
 */
public class FilterAggregationBuilder extends AggregationBuilder<FilterAggregationBuilder> {

    private QueryBuilder filter;

    /**
     * Sole constructor.
     */
    public FilterAggregationBuilder(String name) {
        super(name, InternalFilter.TYPE.name());
    }

    /**
     * Set the filter to use, only documents that match this filter will fall
     * into the bucket defined by this {@link Filter} aggregation.
     */
    public FilterAggregationBuilder filter(QueryBuilder filter) {
        this.filter = filter;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (filter == null) {
            throw new SearchSourceBuilderException("filter must be set on filter aggregation [" + getName() + "]");
        }
        filter.toXContent(builder, params);
        return builder;
    }
}
