/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import java.util.stream.Stream;

public enum HealthStatus {
    GREEN,
    YELLOW,
    RED;

    public static HealthStatus aggregate(Stream<HealthStatus> statusStream) {
        return statusStream.reduce(HealthStatus.GREEN, (healthStatus1, healthStatus2) -> {
            if (healthStatus1.equals(RED) || healthStatus2.equals(RED)) {
                return RED;
            } else if (healthStatus1.equals(YELLOW) || healthStatus2.equals(YELLOW)) {
                return YELLOW;
            } else {
                return GREEN;
            }
        });
    }
}
