/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import java.util.Comparator;
import java.util.stream.Stream;

public enum HealthStatus {
    GREEN((byte) 0),
    YELLOW((byte) 1),
    RED((byte) 2);

    private final byte value;

    HealthStatus(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public static HealthStatus merge(Stream<HealthStatus> statuses) {
        return statuses.max(Comparator.comparing(HealthStatus::value)).orElse(GREEN);
    }
}
