/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.metrics;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class APMMetricUtils {
    public static LongHistogram getOrRegisterLongHistogram(MeterRegistry meterRegistry, String name, String description, String unit) {
        LongHistogram histogram = meterRegistry.getLongHistogram(name);
        return histogram != null ? histogram : meterRegistry.registerLongHistogram(name, description, unit);
    }

    public static LongCounter getOrRegisterLongCounter(MeterRegistry meterRegistry, String name, String description, String unit) {
        LongCounter counter = meterRegistry.getLongCounter(name);
        return counter != null ? counter : meterRegistry.registerLongCounter(name, description, unit);
    }
}
