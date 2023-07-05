/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.util.function.BiFunction;

public class GenerateDownsampleIndexNameStep extends GenerateUniqueIndexNameStep {

    public static final String NAME = "generate-downsample-index-name";
    private final DateHistogramInterval interval;

    public GenerateDownsampleIndexNameStep(
        final StepKey key,
        final StepKey nextStepKey,
        final String prefix,
        final DateHistogramInterval interval,
        final BiFunction<String, LifecycleExecutionState.Builder, LifecycleExecutionState.Builder> lifecycleStateSetter
    ) {
        super(key, nextStepKey, prefix, lifecycleStateSetter);
        this.interval = interval;
    }

    @Override
    public String generateIndexName(final String prefix, final String indexName) {
        return generateDownsampleTargetIndexName(prefix, interval, indexName);
    }

    private static String generateDownsampleTargetIndexName(
        final String prefix,
        final DateHistogramInterval interval,
        final String indexName
    ) {
        return prefix + "-" + indexName + "-" + interval;
    }
}
