/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.context.Context;

public class LoggingDoubleGauge implements io.opentelemetry.api.metrics.DoubleCounter {
    @Override
    public void add(double value) {

    }

    @Override
    public void add(double value, Attributes attributes) {

    }

    @Override
    public void add(double value, Attributes attributes, Context context) {

    }
}
