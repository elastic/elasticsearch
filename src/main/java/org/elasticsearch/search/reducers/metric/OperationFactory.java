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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.search.reducers.metric.multi.delta.Delta;
import org.elasticsearch.search.reducers.metric.multi.stats.Stats;
import org.elasticsearch.search.reducers.metric.single.avg.Avg;
import org.elasticsearch.search.reducers.metric.single.max.Max;
import org.elasticsearch.search.reducers.metric.single.min.Min;
import org.elasticsearch.search.reducers.metric.single.sum.Sum;

import java.util.Map;

public class OperationFactory {
    public static MetricOp get(String opName) {
        if (opName.equals("sum")) {
            return new Sum();
        }
        if (opName.equals("avg")) {
            return new Avg();
        }
        if (opName.equals("min")) {
            return new Min();
        }
        if (opName.equals("max")) {
            return new Max();
        }
        if (opName.equals("delta")) {
            return new Delta();
        }
        if (opName.equals("stats")) {
            return new Stats();
        }
        throw new ElasticsearchParseException("No metric reducer called " + opName);
    }
}
