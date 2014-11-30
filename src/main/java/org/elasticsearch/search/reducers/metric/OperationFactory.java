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
import org.elasticsearch.search.reducers.metric.format.Array;
import org.elasticsearch.search.reducers.metric.numeric.single.delta.Delta;
import org.elasticsearch.search.reducers.metric.numeric.multi.stats.Stats;
import org.elasticsearch.search.reducers.metric.numeric.single.avg.Avg;
import org.elasticsearch.search.reducers.metric.numeric.single.max.Max;
import org.elasticsearch.search.reducers.metric.numeric.single.min.Min;
import org.elasticsearch.search.reducers.metric.numeric.single.sum.Sum;

public class OperationFactory {
    public static MetricOp get(String opName) {
        switch (opName) {
            case "sum": {
                return new Sum();
            }
            case "avg": {
                return new Avg();
            }
            case "min": {
                return new Min();
            }
            case "max": {
                return new Max();
            }
            case "delta": {
                return new Delta();
            }
            case "stats": {
                return new Stats();
            }
            case "array": {
                return new Array();
            }
            default: {
                throw new ElasticsearchParseException("No metric reducer called " + opName);
            }
        }
    }
}
