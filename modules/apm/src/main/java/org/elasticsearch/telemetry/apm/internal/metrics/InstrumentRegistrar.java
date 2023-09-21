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
import org.elasticsearch.telemetry.metric.Instrument;

import java.util.Map;

public class InstrumentRegistrar<T extends Instrument> {
    private final Map<MetricName, T> registered = ConcurrentCollections.newConcurrentMap();

    void register(T instrument) {
        registered.compute(instrument.getName(), (k, v) -> {
            if (v != null) {
                throw new IllegalStateException(
                    instrument.getClass().getSimpleName() + "[" + instrument.getName().getRawName() + "] already registered"
                );
            }

            return instrument;
        });
    }

    T get(MetricName name) {
        return registered.get(name);
    }
}
