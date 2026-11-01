/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Test double for {@link MetricExporter} shared across the otelsdk exporter tests. Records every
 * export and can be toggled to fail subsequent calls.
 */
class FakeMetricExporter implements MetricExporter {

    private final List<MetricData> exported = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean shouldFail;

    void setShouldFail(boolean fail) {
        shouldFail = fail;
    }

    List<String> exportedNames() {
        synchronized (exported) {
            return exported.stream().map(MetricData::getName).toList();
        }
    }

    void clearExported() {
        exported.clear();
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        exported.addAll(metrics);
        return shouldFail ? CompletableResultCode.ofFailure() : CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
        return AggregationTemporality.DELTA;
    }
}
