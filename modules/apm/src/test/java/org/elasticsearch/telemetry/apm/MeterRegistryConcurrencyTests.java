/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.DoubleUpDownCounterBuilder;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.api.metrics.ObservableLongUpDownCounter;

import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class MeterRegistryConcurrencyTests extends ESTestCase {
    private final String name = "es.test.name.total";
    private final String description = "desc";
    private final String unit = "kg";
    private final Meter noopMeter = OpenTelemetry.noop().getMeter("noop");
    private final CountDownLatch buildLatch = new CountDownLatch(1);
    private final CountDownLatch registerLatch = new CountDownLatch(1);
    private final Meter lockingMeter = new Meter() {
        @Override
        public LongCounterBuilder counterBuilder(String name) {
            return null;
        }

        @Override
        public LongUpDownCounterBuilder upDownCounterBuilder(String name) {
            return new LockingLongCounterBuilder();
        }

        @Override
        public DoubleHistogramBuilder histogramBuilder(String name) {
            return null;
        }

        @Override
        public DoubleGaugeBuilder gaugeBuilder(String name) {
            return null;
        }
    };

    class LockingLongCounterBuilder implements LongUpDownCounterBuilder {
        @Override
        public LongUpDownCounterBuilder setDescription(String description) {
            return this;
        }

        @Override
        public LongUpDownCounterBuilder setUnit(String unit) {
            return this;
        }

        @Override
        public DoubleUpDownCounterBuilder ofDoubles() {
            return null;
        }

        @Override
        public LongUpDownCounter build() {
            try {
                buildLatch.countDown();
                registerLatch.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return Mockito.mock(LongUpDownCounter.class);
        }

        @Override
        public ObservableLongUpDownCounter buildWithCallback(Consumer<ObservableLongMeasurement> callback) {
            return null;
        }
    }

    public void testLockingWhenRegistering() throws Exception {
        APMMeterRegistry meterRegistrar = new APMMeterRegistry(lockingMeter);

        var registerThread = new Thread(() -> meterRegistrar.registerLongCounter(name, description, unit));
        // registerThread has a countDown latch that is simulating a long-running registration
        registerThread.start();
        buildLatch.await(30, TimeUnit.SECONDS); // wait for registerThread to hold the lock
        var setProviderThread = new Thread(() -> meterRegistrar.setProvider(noopMeter));
        // a setProviderThread will attempt to override a meter, but will wait to acquireLock
        setProviderThread.start();

        // assert that a thread is waiting for a lock during long-running registration
        assertBusy(() -> assertThat(setProviderThread.getState(), equalTo(Thread.State.WAITING)));
        // assert that the old lockingMeter is still in place
        assertThat(meterRegistrar.getMeter(), sameInstance(lockingMeter));

        // finish long-running registration
        registerLatch.countDown();
        // wait for everything to quiesce, registerLatch.countDown() doesn't ensure lock has been released
        setProviderThread.join(30000);
        registerThread.join(30000);
        // assert that a meter was overriden
        assertThat(meterRegistrar.getMeter(), sameInstance(noopMeter));
    }
}
