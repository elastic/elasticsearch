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

import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketPipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.max.MaxBucketPipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.min.MinBucketPipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketPipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.StatsBucketPipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketPipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.sum.SumBucketPipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptPipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.cumulativesum.CumulativeSumPipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativePipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgPipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.serialdiff.SerialDiffPipelineAggregatorBuilder;

import java.util.Map;

public final class PipelineAggregatorBuilders {

    private PipelineAggregatorBuilders() {
    }

    public static final DerivativePipelineAggregatorBuilder derivative(String name, String bucketsPath) {
        return new DerivativePipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final MaxBucketPipelineAggregatorBuilder maxBucket(String name, String bucketsPath) {
        return new MaxBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final MinBucketPipelineAggregatorBuilder minBucket(String name, String bucketsPath) {
        return new MinBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final AvgBucketPipelineAggregatorBuilder avgBucket(String name, String bucketsPath) {
        return new AvgBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final SumBucketPipelineAggregatorBuilder sumBucket(String name, String bucketsPath) {
        return new SumBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final StatsBucketPipelineAggregatorBuilder statsBucket(String name, String bucketsPath) {
        return new StatsBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final ExtendedStatsBucketPipelineAggregatorBuilder extendedStatsBucket(String name,
            String bucketsPath) {
        return new ExtendedStatsBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final PercentilesBucketPipelineAggregatorBuilder percentilesBucket(String name,
            String bucketsPath) {
        return new PercentilesBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final MovAvgPipelineAggregatorBuilder movingAvg(String name, String bucketsPath) {
        return new MovAvgPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final BucketScriptPipelineAggregatorBuilder bucketScript(String name,
            Map<String, String> bucketsPathsMap, Script script) {
        return new BucketScriptPipelineAggregatorBuilder(name, bucketsPathsMap, script);
    }

    public static final BucketScriptPipelineAggregatorBuilder bucketScript(String name, Script script,
            String... bucketsPaths) {
        return new BucketScriptPipelineAggregatorBuilder(name, script, bucketsPaths);
    }

    public static final BucketSelectorPipelineAggregatorBuilder bucketSelector(String name,
            Map<String, String> bucketsPathsMap, Script script) {
        return new BucketSelectorPipelineAggregatorBuilder(name, bucketsPathsMap, script);
    }

    public static final BucketSelectorPipelineAggregatorBuilder bucketSelector(String name, Script script,
            String... bucketsPaths) {
        return new BucketSelectorPipelineAggregatorBuilder(name, script, bucketsPaths);
    }

    public static final CumulativeSumPipelineAggregatorBuilder cumulativeSum(String name,
            String bucketsPath) {
        return new CumulativeSumPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final SerialDiffPipelineAggregatorBuilder diff(String name, String bucketsPath) {
        return new SerialDiffPipelineAggregatorBuilder(name, bucketsPath);
    }
}
