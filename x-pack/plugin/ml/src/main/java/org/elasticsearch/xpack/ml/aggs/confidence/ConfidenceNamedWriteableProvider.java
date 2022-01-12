/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

import java.util.List;

public final class ConfidenceNamedWriteableProvider {
    private ConfidenceNamedWriteableProvider() {}

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                ConfidenceValue.class,
                SingleMetricConfidenceBuilder.SingleMetricConfidenceValue.NAME,
                SingleMetricConfidenceBuilder.SingleMetricConfidenceValue::new
            ),
            new NamedWriteableRegistry.Entry(
                ConfidenceValue.class,
                MultiMetricConfidenceBuilder.MultiMetricConfidenceValue.NAME,
                MultiMetricConfidenceBuilder.MultiMetricConfidenceValue::new
            ),
            new NamedWriteableRegistry.Entry(
                ConfidenceValue.class,
                ConfidenceValue.BucketConfidenceValue.NAME,
                ConfidenceValue.BucketConfidenceValue::fromStream
            ),
            new NamedWriteableRegistry.Entry(
                ConfidenceValue.class,
                MultiBucketConfidenceBuilder.MultiBucketConfidenceValue.NAME,
                MultiBucketConfidenceBuilder.MultiBucketConfidenceValue::new
            )
        );
    }

}
