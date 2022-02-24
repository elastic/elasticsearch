/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.stream.Stream;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;

public class HealthStatusTests extends ESTestCase {

    public void testAllGreenStatuses() {
        assertEquals(GREEN, HealthStatus.merge(randomStatusesContaining(GREEN)));
    }

    public void testYellowStatus() {
        assertEquals(YELLOW, HealthStatus.merge(randomStatusesContaining(GREEN, YELLOW)));
    }

    public void testRedStatus() {
        assertEquals(RED, HealthStatus.merge(randomStatusesContaining(GREEN, YELLOW, RED)));
    }

    public void testEmpty() {
        assertEquals(GREEN, HealthStatus.merge(Stream.empty()));
    }

    private static Stream<HealthStatus> randomStatusesContaining(HealthStatus... statuses) {
        var result = new ArrayList<HealthStatus>();
        for (HealthStatus status : statuses) {
            result.addAll(randomList(1, 10, () -> status));
        }
        return result.stream();
    }
}
