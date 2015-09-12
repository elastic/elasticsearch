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

import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.max.MaxBucketBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.min.MinBucketBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.StatsBucketBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.sum.SumBucketBuilder;
import org.elasticsearch.search.aggregations.pipeline.cumulativesum.CumulativeSumBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptBuilder;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativeBuilder;
import org.elasticsearch.search.aggregations.pipeline.having.BucketSelectorBuilder;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgBuilder;
import org.elasticsearch.search.aggregations.pipeline.serialdiff.SerialDiffBuilder;

public final class PipelineAggregatorBuilders {

    private PipelineAggregatorBuilders() {
    }

    public static final DerivativeBuilder derivative(String name) {
        return new DerivativeBuilder(name);
    }

    public static final MaxBucketBuilder maxBucket(String name) {
        return new MaxBucketBuilder(name);
    }

    public static final MinBucketBuilder minBucket(String name) {
        return new MinBucketBuilder(name);
    }

    public static final AvgBucketBuilder avgBucket(String name) {
        return new AvgBucketBuilder(name);
    }

    public static final SumBucketBuilder sumBucket(String name) {
        return new SumBucketBuilder(name);
    }

    public static final StatsBucketBuilder statsBucket(String name) {
        return new StatsBucketBuilder(name);
    }

    public static final ExtendedStatsBucketBuilder extendedStatsBucket(String name) {
        return new ExtendedStatsBucketBuilder(name);
    }

    public static final PercentilesBucketBuilder percentilesBucket(String name) {
        return new PercentilesBucketBuilder(name);
    }

    public static final MovAvgBuilder movingAvg(String name) {
        return new MovAvgBuilder(name);
    }

    public static final BucketScriptBuilder bucketScript(String name) {
        return new BucketScriptBuilder(name);
    }

    public static final BucketSelectorBuilder having(String name) {
        return new BucketSelectorBuilder(name);
    }

    public static final CumulativeSumBuilder cumulativeSum(String name) {
        return new CumulativeSumBuilder(name);
    }

    public static final SerialDiffBuilder diff(String name) {
        return new SerialDiffBuilder(name);
    }
}
