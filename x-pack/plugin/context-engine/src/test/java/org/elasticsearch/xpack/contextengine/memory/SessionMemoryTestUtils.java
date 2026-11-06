/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public final class SessionMemoryTestUtils {

    private SessionMemoryTestUtils() {}

    public static SessionMemory randomSessionMemory() {
        return new SessionMemory(
            randomBoolean() ? null : randomTimestamp(),
            randomBoolean() ? null : ESTestCase.randomAlphaOfLength(10),
            randomBoolean() ? null : ESTestCase.randomAlphaOfLength(8),
            randomBoolean() ? null : ESTestCase.randomList(0, 3, () -> ESTestCase.randomAlphaOfLength(6)),
            ESTestCase.randomAlphaOfLength(12),
            ESTestCase.randomAlphaOfLength(8),
            ESTestCase.randomAlphaOfLengthBetween(1, 50),
            randomBoolean() ? null : randomMedia(),
            randomBoolean() ? null : randomAttributes()
        );
    }

    public static List<Map<String, Object>> randomMedia() {
        return ESTestCase.randomList(
            1,
            3,
            () -> Map.of(
                "type",
                ESTestCase.randomFrom("image", "pdf"),
                "value",
                "data:image/png;base64," + ESTestCase.randomAlphaOfLength(16)
            )
        );
    }

    public static Instant randomTimestamp() {
        return Instant.ofEpochMilli(ESTestCase.randomLongBetween(0, 4_102_444_800_000L));
    }

    public static Map<String, Object> randomAttributes() {
        return ESTestCase.randomMap(0, 4, () -> Tuple.tuple(ESTestCase.randomAlphaOfLength(5), ESTestCase.randomAlphaOfLength(5)));
    }

    public static List<String> randomAiIndexIds() {
        return ESTestCase.randomList(0, 3, () -> ESTestCase.randomAlphaOfLength(6));
    }

    private static boolean randomBoolean() {
        return ESTestCase.randomBoolean();
    }
}
