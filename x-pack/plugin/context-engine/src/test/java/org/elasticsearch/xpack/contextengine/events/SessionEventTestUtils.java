/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.events;

import org.elasticsearch.test.ESTestCase;

import java.time.Instant;

public final class SessionEventTestUtils {

    private SessionEventTestUtils() {}

    public static SessionEvent randomSessionEvent() {
        return new SessionEvent(
            ESTestCase.randomBoolean() ? null : randomTimestamp(),
            ESTestCase.randomBoolean() ? null : ESTestCase.randomAlphaOfLength(10),
            ESTestCase.randomBoolean() ? null : ESTestCase.randomAlphaOfLength(8),
            ESTestCase.randomBoolean() ? null : ESTestCase.randomList(0, 3, () -> ESTestCase.randomAlphaOfLength(6)),
            ESTestCase.randomAlphaOfLength(12),
            ESTestCase.randomBoolean() ? null : ESTestCase.randomAlphaOfLength(8),
            ESTestCase.randomAlphaOfLength(8),
            ESTestCase.randomAlphaOfLengthBetween(1, 200)
        );
    }

    public static Instant randomTimestamp() {
        return Instant.ofEpochMilli(ESTestCase.randomLongBetween(0, 4_102_444_800_000L));
    }
}
