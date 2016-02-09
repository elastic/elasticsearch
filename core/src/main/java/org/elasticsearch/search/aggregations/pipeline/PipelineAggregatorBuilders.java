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
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.max.MaxBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.min.MinBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.StatsBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.sum.SumBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.cumulativesum.CumulativeSumPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativePipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.having.BucketSelectorPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.serialdiff.SerialDiffPipelineAggregator;

import java.util.Map;

public final class PipelineAggregatorBuilders {

    private PipelineAggregatorBuilders() {
    }

    public static final DerivativePipelineAggregator.DerivativePipelineAggregatorBuilder derivative(String name, String bucketsPath) {
        return new DerivativePipelineAggregator.DerivativePipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final MaxBucketPipelineAggregator.MaxBucketPipelineAggregatorBuilder maxBucket(String name, String bucketsPath) {
        return new MaxBucketPipelineAggregator.MaxBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final MinBucketPipelineAggregator.MinBucketPipelineAggregatorBuilder minBucket(String name, String bucketsPath) {
        return new MinBucketPipelineAggregator.MinBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final AvgBucketPipelineAggregator.AvgBucketPipelineAggregatorBuilder avgBucket(String name, String bucketsPath) {
        return new AvgBucketPipelineAggregator.AvgBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final SumBucketPipelineAggregator.SumBucketPipelineAggregatorBuilder sumBucket(String name, String bucketsPath) {
        return new SumBucketPipelineAggregator.SumBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final StatsBucketPipelineAggregator.StatsBucketPipelineAggregatorBuilder statsBucket(String name, String bucketsPath) {
        return new StatsBucketPipelineAggregator.StatsBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final ExtendedStatsBucketPipelineAggregator.ExtendedStatsBucketPipelineAggregatorBuilder extendedStatsBucket(String name,
            String bucketsPath) {
        return new ExtendedStatsBucketPipelineAggregator.ExtendedStatsBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final PercentilesBucketPipelineAggregator.PercentilesBucketPipelineAggregatorBuilder percentilesBucket(String name,
            String bucketsPath) {
        return new PercentilesBucketPipelineAggregator.PercentilesBucketPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final MovAvgPipelineAggregator.MovAvgPipelineAggregatorBuilder movingAvg(String name, String bucketsPath) {
        return new MovAvgPipelineAggregator.MovAvgPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final BucketScriptPipelineAggregator.BucketScriptPipelineAggregatorBuilder bucketScript(String name,
            Map<String, String> bucketsPathsMap, Script script) {
        return new BucketScriptPipelineAggregator.BucketScriptPipelineAggregatorBuilder(name, bucketsPathsMap, script);
    }

    public static final BucketScriptPipelineAggregator.BucketScriptPipelineAggregatorBuilder bucketScript(String name, Script script,
            String... bucketsPaths) {
        return new BucketScriptPipelineAggregator.BucketScriptPipelineAggregatorBuilder(name, script, bucketsPaths);
    }

    public static final BucketSelectorPipelineAggregator.BucketSelectorPipelineAggregatorBuilder bucketSelector(String name,
            Map<String, String> bucketsPathsMap, Script script) {
        return new BucketSelectorPipelineAggregator.BucketSelectorPipelineAggregatorBuilder(name, bucketsPathsMap, script);
    }

    public static final BucketSelectorPipelineAggregator.BucketSelectorPipelineAggregatorBuilder bucketSelector(String name, Script script,
            String... bucketsPaths) {
        return new BucketSelectorPipelineAggregator.BucketSelectorPipelineAggregatorBuilder(name, script, bucketsPaths);
    }

    public static final CumulativeSumPipelineAggregator.CumulativeSumPipelineAggregatorBuilder cumulativeSum(String name,
            String bucketsPath) {
        return new CumulativeSumPipelineAggregator.CumulativeSumPipelineAggregatorBuilder(name, bucketsPath);
    }

    public static final SerialDiffPipelineAggregator.SerialDiffPipelineAggregatorBuilder diff(String name, String bucketsPath) {
        return new SerialDiffPipelineAggregator.SerialDiffPipelineAggregatorBuilder(name, bucketsPath);
    }
}
