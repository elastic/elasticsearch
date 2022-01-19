/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2.indexer.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.rollup.v2.FieldValueFetcher;

import java.io.IOException;

public class NumberMetricField extends MetricField {
    public NumberMetricField(String fieldName, MetricCollector[] collectors, FieldValueFetcher fetcher) {
        super(fieldName, collectors, fetcher);
    }

    @Override
    public void collectMetric(StreamInput in) throws IOException {
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            double value = in.readDouble();
            for (MetricCollector metricCollector : collectors) {
                metricCollector.collect(value);
            }
        }
    }
}
