/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.metrics.spi;

import io.opentelemetry.sdk.metrics.export.MetricReader;

/**
 * SPI that allows other Elasticsearch plugins to extend the OTel metrics pipeline by providing an extra {@link MetricReader} instance that
 * will be included in it.
 *
 * <p>Max one instance of this SPI is allowed.
 */
public interface MetricReaderProvider {

    /** The {@link MetricReader} to be installed in the OTel metrics SDK. */
    MetricReader getMetricReader();
}
