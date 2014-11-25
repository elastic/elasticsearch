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

package org.elasticsearch.search.reducers.metric.stats;

import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;

import java.util.Map;


public class InternalStats extends org.elasticsearch.search.aggregations.metrics.stats.InternalStats implements Stats {

    public InternalStats(String name, long count, double sum, double min, double max, Map<String, Object> metaData) {
        super(name, count, sum, min, max, metaData);
    }

    @Override
    public InternalStats reduce(InternalAggregation.ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Not supported");
    }

}
