/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry;

import java.util.Map;
import java.util.Objects;

public record Measurement(Number value, Map<String, Object> attributes, boolean isDouble) {
    public Measurement {
        Objects.requireNonNull(value);
    }

    public boolean isLong() {
        return isDouble == false;
    }

    public double getDouble() {
        assert isDouble;
        return value.doubleValue();
    }

    public long getLong() {
        assert isLong();
        return value.longValue();
    }
}
