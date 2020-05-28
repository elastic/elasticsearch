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

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.util.Map;

public class AvgBucketPipelineAggregator extends BucketMetricsPipelineAggregator {
    private int count = 0;
    private double sum = 0;

    AvgBucketPipelineAggregator(String name, String[] bucketsPaths, GapPolicy gapPolicy, DocValueFormat format,
            Map<String, Object> metadata) {
        super(name, bucketsPaths, gapPolicy, format, metadata);
    }

    @Override
    protected void preCollection() {
        count = 0;
        sum = 0;
    }

    @Override
    protected void collectBucketValue(String bucketKey, Double bucketValue) {
        count++;
        sum += bucketValue;
    }

    @Override
    protected InternalAggregation buildAggregation(Map<String, Object> metadata) {
        double avgValue = count == 0 ? Double.NaN : (sum / count);
        return new InternalSimpleValue(name(), avgValue, format, metadata);
    }

}
