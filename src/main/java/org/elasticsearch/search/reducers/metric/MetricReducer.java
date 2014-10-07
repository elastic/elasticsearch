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

package org.elasticsearch.search.reducers.metric;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMetricsAggregation;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerFactories;
import org.elasticsearch.search.reducers.ReductionExecutionException;

import java.util.List;

public abstract class MetricReducer extends Reducer {

    public MetricReducer(String name, ReducerFactories factories, SearchContext context, Reducer parent) {
        super(name, factories, context, parent);
    }

    @Override
    public abstract InternalMetricsAggregation reduce(List<MultiBucketsAggregation> aggregations, SearchContext context) throws ReductionExecutionException;
}
