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


import org.elasticsearch.search.reducers.bucket.slidingwindow.SlidingWindowBuilder;
import org.elasticsearch.search.reducers.bucket.union.UnionBuilder;
import org.elasticsearch.search.reducers.metric.MetricsBuilder;
import org.elasticsearch.search.reducers.metric.format.Array;
import org.elasticsearch.search.reducers.metric.numeric.single.delta.Delta;
import org.elasticsearch.search.reducers.metric.numeric.multi.stats.Stats;
import org.elasticsearch.search.reducers.metric.numeric.single.avg.Avg;
import org.elasticsearch.search.reducers.metric.numeric.single.max.Max;
import org.elasticsearch.search.reducers.metric.numeric.single.min.Min;
import org.elasticsearch.search.reducers.metric.numeric.single.sum.Sum;

public class ReducerBuilders {

    public static SlidingWindowBuilder slidingWindowReducer(String name) {
        return new SlidingWindowBuilder(name);
    }

    public static UnionBuilder unionReducer(String name) {
        return new UnionBuilder(name);
    }

    public static MetricsBuilder sumReducer(String name) {
        return new Sum.SumBuilder(name);
    }

    public static MetricsBuilder avgReducer(String name) {
        return new Avg.AvgBuilder(name);
    }

    public static MetricsBuilder minReducer(String name) {
        return new Min.MinBuilder(name);
    }

    public static MetricsBuilder maxReducer(String name) {
        return new Max.MaxBuilder(name);
    }

    public static Delta.DeltaBuilder deltaReducer(String name) {
        return new Delta.DeltaBuilder(name);
    }

    public static Stats.StatsBuilder statsReducer(String name) {
        return new Stats.StatsBuilder(name);
    }

    public static Array.ArrayBuilder arrayReducer(String name) {
        return new Array.ArrayBuilder(name);
    }
}
