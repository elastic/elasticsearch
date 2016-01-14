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

    public static final DerivativePipelineAggregator.Factory derivative(String name, String bucketsPath) {
        return new DerivativePipelineAggregator.Factory(name, bucketsPath);
    }

    public static final MaxBucketPipelineAggregator.Factory maxBucket(String name, String bucketsPath) {
        return new MaxBucketPipelineAggregator.Factory(name, bucketsPath);
    }

    public static final MinBucketPipelineAggregator.Factory minBucket(String name, String bucketsPath) {
        return new MinBucketPipelineAggregator.Factory(name, bucketsPath);
    }

    public static final AvgBucketPipelineAggregator.Factory avgBucket(String name, String bucketsPath) {
        return new AvgBucketPipelineAggregator.Factory(name, bucketsPath);
    }

    public static final SumBucketPipelineAggregator.Factory sumBucket(String name, String bucketsPath) {
        return new SumBucketPipelineAggregator.Factory(name, bucketsPath);
    }

    public static final StatsBucketPipelineAggregator.Factory statsBucket(String name, String bucketsPath) {
        return new StatsBucketPipelineAggregator.Factory(name, bucketsPath);
    }

    public static final ExtendedStatsBucketPipelineAggregator.Factory extendedStatsBucket(String name, String bucketsPath) {
        return new ExtendedStatsBucketPipelineAggregator.Factory(name, bucketsPath);
    }

    public static final PercentilesBucketPipelineAggregator.Factory percentilesBucket(String name, String bucketsPath) {
        return new PercentilesBucketPipelineAggregator.Factory(name, bucketsPath);
    }

    public static final MovAvgPipelineAggregator.Factory movingAvg(String name, String bucketsPath) {
        return new MovAvgPipelineAggregator.Factory(name, bucketsPath);
    }

    public static final BucketScriptPipelineAggregator.Factory bucketScript(String name, Map<String, String> bucketsPathsMap,
            Script script) {
        return new BucketScriptPipelineAggregator.Factory(name, bucketsPathsMap, script);
    }

    public static final BucketScriptPipelineAggregator.Factory bucketScript(String name, Script script, String... bucketsPaths) {
        return new BucketScriptPipelineAggregator.Factory(name, script, bucketsPaths);
    }

    public static final BucketSelectorPipelineAggregator.Factory bucketSelector(String name, Map<String, String> bucketsPathsMap,
            Script script) {
        return new BucketSelectorPipelineAggregator.Factory(name, bucketsPathsMap, script);
    }

    public static final BucketSelectorPipelineAggregator.Factory bucketSelector(String name, Script script, String... bucketsPaths) {
        return new BucketSelectorPipelineAggregator.Factory(name, script, bucketsPaths);
    }

    public static final CumulativeSumPipelineAggregator.Factory cumulativeSum(String name, String bucketsPath) {
        return new CumulativeSumPipelineAggregator.Factory(name, bucketsPath);
    }

    public static final SerialDiffPipelineAggregator.Factory diff(String name, String bucketsPath) {
        return new SerialDiffPipelineAggregator.Factory(name, bucketsPath);
    }
}
