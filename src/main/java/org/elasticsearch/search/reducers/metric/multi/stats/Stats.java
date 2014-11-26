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

package org.elasticsearch.search.reducers.metric.multi.stats;

import org.elasticsearch.search.reducers.ReductionExecutionException;
import org.elasticsearch.search.reducers.metric.MetricOp;

public class Stats implements MetricOp {

    public StatsResult op(Object[] bucketProperties) throws ReductionExecutionException {
        double sum = 0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        for (Object bucketValue : bucketProperties) {
            double bucketDoubleValue = ((Number)bucketValue).doubleValue();
            sum += bucketDoubleValue;
            if (bucketDoubleValue < min) {
                min = bucketDoubleValue;
            }
            if (bucketDoubleValue > max) {
                max = bucketDoubleValue;
            }
        }

        return new StatsResult(bucketProperties.length, sum, min, max);
    }

}
