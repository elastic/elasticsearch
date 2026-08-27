/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.telemetry;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransformMeterRegistryTests extends ESTestCase {

    public void testCreateRegistersBothCounters() {
        var meterRegistry = mock(MeterRegistry.class);
        var autoMigration = mock(LongCounter.class);
        var missingCredentials = mock(LongCounter.class);
        when(meterRegistry.registerLongCounter(anyString(), anyString(), anyString())).thenReturn(autoMigration);
        when(meterRegistry.registerLongCounter(eq("es.transform.missing_credentials.count.total"), anyString(), anyString())).thenReturn(
            missingCredentials
        );

        var registry = TransformMeterRegistry.create(meterRegistry);

        verify(meterRegistry).registerLongCounter(eq("es.transform.automigration.count.total"), anyString(), eq("count"));
        verify(meterRegistry).registerLongCounter(eq("es.transform.missing_credentials.count.total"), anyString(), eq("count"));
        assertThat(registry.runningWithoutCredentialsCount(), sameInstance(missingCredentials));
    }

    public void testNoOp() {
        var registry = TransformMeterRegistry.noOp();
        assertThat(registry.autoMigrationCount(), sameInstance(LongCounter.NOOP));
        assertThat(registry.runningWithoutCredentialsCount(), sameInstance(LongCounter.NOOP));
    }
}
