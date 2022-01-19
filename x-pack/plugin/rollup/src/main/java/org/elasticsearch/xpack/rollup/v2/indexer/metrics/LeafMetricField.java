/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2.indexer.metrics;

import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;

public abstract class LeafMetricField {
    protected final MetricCollector[] metricCollectors;

    public LeafMetricField(MetricCollector[] metricCollectors) {
        this.metricCollectors = metricCollectors;
    }

    public abstract void collectMetric(int docID) throws IOException;

    public abstract void writeMetrics(int docID, BytesStreamOutput out) throws IOException;
}
