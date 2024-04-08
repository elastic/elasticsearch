/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketScriptPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.ExtendedStatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.MinBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.SerialDiffPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.SumBucketPipelineAggregationBuilder;

import java.util.Map;

public final class PipelineAggregatorBuilders {

    private PipelineAggregatorBuilders() {}

    public static MaxBucketPipelineAggregationBuilder maxBucket(String name, String bucketsPath) {
        return new MaxBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public static MinBucketPipelineAggregationBuilder minBucket(String name, String bucketsPath) {
        return new MinBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public static AvgBucketPipelineAggregationBuilder avgBucket(String name, String bucketsPath) {
        return new AvgBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public static SumBucketPipelineAggregationBuilder sumBucket(String name, String bucketsPath) {
        return new SumBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public static StatsBucketPipelineAggregationBuilder statsBucket(String name, String bucketsPath) {
        return new StatsBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public static ExtendedStatsBucketPipelineAggregationBuilder extendedStatsBucket(String name, String bucketsPath) {
        return new ExtendedStatsBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public static PercentilesBucketPipelineAggregationBuilder percentilesBucket(String name, String bucketsPath) {
        return new PercentilesBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public static BucketScriptPipelineAggregationBuilder bucketScript(String name, Map<String, String> bucketsPathsMap, Script script) {
        return new BucketScriptPipelineAggregationBuilder(name, bucketsPathsMap, script);
    }

    public static BucketScriptPipelineAggregationBuilder bucketScript(String name, Script script, String... bucketsPaths) {
        return new BucketScriptPipelineAggregationBuilder(name, script, bucketsPaths);
    }

    public static SerialDiffPipelineAggregationBuilder diff(String name, String bucketsPath) {
        return new SerialDiffPipelineAggregationBuilder(name, bucketsPath);
    }
}
