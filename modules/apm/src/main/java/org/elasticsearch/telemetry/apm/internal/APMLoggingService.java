/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.sdk.common.CompletableResultCode;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkExportLogsSupplier;

import java.io.Closeable;

/**
 * Manages the lifecycle of the OTel SDK audit-log export path.
 * Analogous to {@link APMMeterService} (metrics) and {@link org.elasticsearch.telemetry.apm.internal.tracing.APMTracer} (traces).
 */
public class APMLoggingService implements Closeable {

    private final OtelSdkExportLogsSupplier supplier;

    public APMLoggingService(Settings settings) {
        supplier = new OtelSdkExportLogsSupplier(settings);
        supplier.install();
    }

    public CompletableResultCode forceFlush() {
        return supplier.forceFlush();
    }

    @Override
    public void close() {
        supplier.close();
    }
}
