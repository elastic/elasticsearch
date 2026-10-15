/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeploymentPathUnsafeIdTelemetryTests extends ESTestCase {

    public void testRecordPathUnsafeDeploymentIdStartShouldIncrementCounter() {
        MeterRegistry meterRegistry = mock(MeterRegistry.class);
        LongCounter counter = mock(LongCounter.class);
        when(meterRegistry.registerLongCounter(eq(DeploymentPathUnsafeIdTelemetry.PATH_UNSAFE_ID_START_METRIC), anyString(), anyString()))
            .thenReturn(counter);

        DeploymentPathUnsafeIdTelemetry telemetry = new DeploymentPathUnsafeIdTelemetry(meterRegistry);
        telemetry.recordPathUnsafeDeploymentIdStart();

        verify(counter).incrementBy(1);
    }

    public void testNoopShouldNotRegisterMetrics() {
        DeploymentPathUnsafeIdTelemetry.NOOP.recordPathUnsafeDeploymentIdStart();
    }
}
