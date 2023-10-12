/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics.internal.metrics;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class MeterRegistryRegistrarTests extends ESTestCase {
    Meter noopMeter = OpenTelemetry.noop().getMeter("noop");
    Meter someOtherMeter = OpenTelemetry.noop().getMeter("xyz");
    String name = "name";
    String description = "desc";
    String unit = "kg";

    public void testRegistrationAndLookup() {
        APMMeterRegistry meterRegistrar = new APMMeterRegistry(noopMeter);
        {
            var registered = meterRegistrar.registerDoubleCounter(name, description, unit);
            var lookedUp = meterRegistrar.getDoubleCounter(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = meterRegistrar.registerDoubleUpDownCounter(name, description, unit);
            var lookedUp = meterRegistrar.getDoubleUpDownCounter(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = meterRegistrar.registerDoubleGauge(name, description, unit);
            var lookedUp = meterRegistrar.getDoubleGauge(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = meterRegistrar.registerDoubleHistogram(name, description, unit);
            var lookedUp = meterRegistrar.getDoubleHistogram(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = meterRegistrar.registerLongCounter(name, description, unit);
            var lookedUp = meterRegistrar.getLongCounter(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = meterRegistrar.registerLongUpDownCounter(name, description, unit);
            var lookedUp = meterRegistrar.getLongUpDownCounter(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = meterRegistrar.registerLongGauge(name, description, unit);
            var lookedUp = meterRegistrar.getLongGauge(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = meterRegistrar.registerLongHistogram(name, description, unit);
            var lookedUp = meterRegistrar.getLongHistogram(name);
            assertThat(registered, sameInstance(lookedUp));
        }
    }

    public void testNameValidation() {
        APMMeterRegistry meterRegistrar = new APMMeterRegistry(noopMeter);

        meterRegistrar.registerLongHistogram(name, description, unit);
        var e = expectThrows(IllegalStateException.class, () -> meterRegistrar.registerLongHistogram(name, description, unit));
        assertThat(e.getMessage(), equalTo("LongHistogramAdapter[name] already registered"));
    }
}
