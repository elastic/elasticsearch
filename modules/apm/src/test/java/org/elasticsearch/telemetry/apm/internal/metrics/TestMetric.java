/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import java.util.Map;
import java.util.Objects;

public record TestMetric(Number number, Map<String, Object> attributes) {
    public TestMetric {
        Objects.requireNonNull(number);
    }
}
