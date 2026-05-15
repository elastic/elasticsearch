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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test double for {@link MetricExporter} shared across the otelsdk exporter tests. Records every
 * export, can be toggled to fail subsequent calls, and can be gated open/closed to make tests
 * observe in-flight exports.
 */
class FakeMetricExporter implements MetricExporter {

    private final List<MetricData> exported = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger inflight = new AtomicInteger();
    private volatile boolean shouldFail;
    private boolean blocked;

    void setShouldFail(boolean fail) {
        shouldFail = fail;
    }

    synchronized void block() {
        blocked = true;
    }

    synchronized void release() {
        blocked = false;
        notifyAll();
    }

    int inflight() {
        return inflight.get();
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
        inflight.incrementAndGet();
        try {
            synchronized (this) {
                while (blocked) {
                    wait();
                }
            }
            exported.addAll(metrics);
            return shouldFail ? CompletableResultCode.ofFailure() : CompletableResultCode.ofSuccess();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CompletableResultCode.ofFailure();
        } finally {
            inflight.decrementAndGet();
        }
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
