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
import org.elasticsearch.search.reducers.metric.multi.delta.DeltaBuilder;

public class ReducerBuilders {

    public static SlidingWindowBuilder slidingWindowReducer(String name) {
        return new SlidingWindowBuilder(name);
    }

    public static UnionBuilder unionReducer(String name) {
        return new UnionBuilder(name);
    }

    public static MetricsBuilder sumReducer(String name) {
        return new MetricsBuilder(name, "sum");
    }

    public static MetricsBuilder avgReducer(String name) {
        return new MetricsBuilder(name, "avg");
    }

    public static MetricsBuilder minReducer(String name) {
        return new MetricsBuilder(name, "min");
    }

    public static MetricsBuilder maxReducer(String name) {
        return new MetricsBuilder(name, "max");
    }

    public static DeltaBuilder deltaReducer(String name) {
        return new DeltaBuilder(name);
    }

    public static MetricsBuilder statsReducer(String name) {
        return new MetricsBuilder(name, "stats");
    }
}
