/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class InstrumentsTests extends ESTestCase {
    Meter noopMeter = OpenTelemetry.noop().getMeter("noop");
    Meter someOtherMeter = OpenTelemetry.noop().getMeter("xyz");
    String name = "name";
    String description = "desc";
    String unit = "kg";

    public void testRegistrationAndLookup() {
        Instruments instruments = new Instruments(noopMeter);
        {
            var registered = instruments.registerDoubleCounter(name, description, unit);
            var lookedUp = instruments.getDoubleCounter(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = instruments.registerDoubleUpDownCounter(name, description, unit);
            var lookedUp = instruments.getDoubleUpDownCounter(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = instruments.registerDoubleGauge(name, description, unit);
            var lookedUp = instruments.getDoubleGauge(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = instruments.registerDoubleHistogram(name, description, unit);
            var lookedUp = instruments.getDoubleHistogram(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = instruments.registerLongCounter(name, description, unit);
            var lookedUp = instruments.getLongCounter(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = instruments.registerLongUpDownCounter(name, description, unit);
            var lookedUp = instruments.getLongUpDownCounter(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = instruments.registerLongGauge(name, description, unit);
            var lookedUp = instruments.getLongGauge(name);
            assertThat(registered, sameInstance(lookedUp));
        }
        {
            var registered = instruments.registerLongHistogram(name, description, unit);
            var lookedUp = instruments.getLongHistogram(name);
            assertThat(registered, sameInstance(lookedUp));
        }
    }

    public void testNameValidation() {
        Instruments instruments = new Instruments(noopMeter);

        instruments.registerLongHistogram(name, description, unit);
        var e = expectThrows(IllegalStateException.class, () -> instruments.registerLongHistogram(name, description, unit));
        assertThat(e.getMessage(), equalTo("LongHistogramAdapter[name] already registered"));
    }
}
