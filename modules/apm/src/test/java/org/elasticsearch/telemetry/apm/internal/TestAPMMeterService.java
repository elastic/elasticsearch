/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.settings.Settings;

import java.util.function.Supplier;

public class TestAPMMeterService extends APMMeterService {
    public TestAPMMeterService(Settings settings, Supplier<Meter> otelMeterSupplier, Supplier<Meter> noopMeterSupplier) {
        super(settings, otelMeterSupplier, noopMeterSupplier);
    }

    public void setEnabled(boolean enabled) {
        // expose pkg private for testing
        super.setEnabled(enabled);
    }
}
