/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.telemetry.MetricName;

import java.util.Map;

public class InstrumentRegistrar<T extends AbstractInstrument<?, ?>> {
    private final Map<MetricName, T> registered = ConcurrentCollections.newConcurrentMap();

    T register(T instrument) {
        registered.compute(instrument.getMetricName(), (k, v) -> {
            if (v != null) {
                throw new IllegalStateException(
                    instrument.getClass().getSimpleName() + "[" + instrument.getName() + "] already registered"
                );
            }

            return instrument;
        });
        return instrument;
    }

    T get(MetricName name) {
        return registered.get(name);
    }
}
