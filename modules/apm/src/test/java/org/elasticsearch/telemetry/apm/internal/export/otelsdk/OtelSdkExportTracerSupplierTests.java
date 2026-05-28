/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_TRACES_ENABLED_SYSTEM_PROPERTY;
import static org.hamcrest.Matchers.containsString;

public class OtelSdkExportTracerSupplierTests extends ESTestCase {

    public void testConstructorWithoutEndpointThrows() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> new OtelSdkExportTracerSupplier(Settings.EMPTY));
        assertThat(e.getMessage(), containsString(OTEL_TRACES_ENABLED_SYSTEM_PROPERTY));
        assertThat(e.getMessage(), containsString("telemetry.otel.traces.endpoint"));
    }

    public void testConstructorWithEmptyEndpointThrows() {
        Settings settings = Settings.builder().put(OtelSdkSettings.TELEMETRY_OTEL_TRACES_ENDPOINT.getKey(), "").build();
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> new OtelSdkExportTracerSupplier(settings));
        assertThat(e.getMessage(), containsString(OTEL_TRACES_ENABLED_SYSTEM_PROPERTY));
        assertThat(e.getMessage(), containsString("telemetry.otel.traces.endpoint"));
    }
}
