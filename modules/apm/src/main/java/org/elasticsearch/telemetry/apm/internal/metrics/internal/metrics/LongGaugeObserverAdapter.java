/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics.internal.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongGaugeBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.telemetry.metric.LongAttributes;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class LongGaugeObserverAdapter extends AbstractInstrument<ObservableLongGauge>
    implements
        org.elasticsearch.telemetry.metric.LongGaugeObserver {
    private static final Logger LOGGER = LogManager.getLogger(LongGaugeObserverAdapter.class);
    private final Function<LongGaugeBuilder, ObservableLongGauge> setCallback;
    private final ReleasableLock closedLock = new ReleasableLock(new ReentrantLock());
    private boolean closed = false;

    public LongGaugeObserverAdapter(Meter meter, String name, String description, String unit, LongSupplier observe) {
        super(meter, name, description, unit);
        setCallback = b -> b.buildWithCallback(cb -> {
            LOGGER.warn("STU, callback in [" + name + "] long only");
            cb.record(observe.getAsLong());
        });
    }

    public LongGaugeObserverAdapter(
        Meter meter,
        String name,
        String description,
        String unit,
        LongSupplier observe,
        Map<String, Object> attributes
    ) {
        super(meter, name, description, unit);
        if (Objects.requireNonNull(attributes).isEmpty()) {
            throw new IllegalArgumentException("attributes must non-empty");
        }
        Attributes at = OtelHelper.fromMap(attributes);
        setCallback = b -> b.buildWithCallback(cb -> {
            LOGGER.warn("STU, callback in [" + name + "] long w/ attributes");
            cb.record(observe.getAsLong(), at);
        });
        setProvider(meter);
    }

    public LongGaugeObserverAdapter(Meter meter, String name, String description, String unit, Supplier<LongAttributes> observe) {
        super(meter, name, description, unit);
        setCallback = b -> {
            LongAttributes da = observe.get();
            return b.buildWithCallback(cb -> {
                LOGGER.warn("STU, callback in [" + name + "] longattributes supplier");
                cb.record(da.value(), OtelHelper.fromMap(da.attributes()));
            });
        };
    }

    private static LongGaugeBuilder builder(Meter meter, String name, String description, String unit) {
        return Objects.requireNonNull(meter).gaugeBuilder(name).setDescription(description).setUnit(unit).ofLongs();
    }

    @Override
    public void close() throws Exception {
        try (ReleasableLock lock = closedLock.acquire()) {
            if (closed == false) {
                getInstrument().close();
            }
            closed = true;
        }
    }

    @Override
    ObservableLongGauge buildInstrument(Meter meter) {
        if (closedLock == null) {
            return null;
        }
        try (ReleasableLock lock = closedLock.acquire()) {
            if (closed) {
                return null;
            }
            LOGGER.warn("STU, building long gauge observer adapter instrument");
            return setCallback.apply(builder(meter, getName(), getDescription(), getUnit()));
        }
    }
}
