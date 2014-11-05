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
package org.elasticsearch.search.reducers;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.reducers.bucket.range.InternalRange;
import org.elasticsearch.search.reducers.bucket.range.RangeReducer;
import org.elasticsearch.search.reducers.bucket.slidingwindow.InternalSlidingWindow;
import org.elasticsearch.search.reducers.bucket.slidingwindow.SlidingWindowReducer;
import org.elasticsearch.search.reducers.metric.avg.AvgReducer;
import org.elasticsearch.search.reducers.metric.avg.InternalAvg;
import org.elasticsearch.search.reducers.metric.cusum.CusumReducer;
import org.elasticsearch.search.reducers.metric.cusum.InternalCusum;
import org.elasticsearch.search.reducers.metric.delta.DeltaReducer;
import org.elasticsearch.search.reducers.metric.delta.InternalDelta;
import org.elasticsearch.search.reducers.metric.max.MaxReducer;
import org.elasticsearch.search.reducers.metric.min.MinReducer;
import org.elasticsearch.search.reducers.metric.stats.StatsReducer;
import org.elasticsearch.search.reducers.metric.sum.SumReducer;

/**
 * A module that registers all the transport streams for the addAggregation
 */
public class TransportReductionModule extends AbstractModule {

    @Override
    protected void configure() {

        InternalSlidingWindow.registerStreams();
        InternalDelta.registerStreams();
        InternalAvg.registerStreams();
        InternalMax.registerStreams();
        InternalMin.registerStreams();
        InternalSum.registerStreams();
        InternalStats.registerStreams();
        InternalRange.registerStreams();
        InternalCusum.registerStreams();
        
        SlidingWindowReducer.registerStreams();
        DeltaReducer.registerStreams();
        AvgReducer.registerStreams();
        MaxReducer.registerStreams();
        MinReducer.registerStreams();
        SumReducer.registerStreams();
        StatsReducer.registerStreams();
        RangeReducer.registerStreams();
        CusumReducer.registerStreams();
    }
}
