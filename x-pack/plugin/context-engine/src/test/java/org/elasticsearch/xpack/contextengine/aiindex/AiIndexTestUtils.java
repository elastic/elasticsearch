/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex;

import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public final class AiIndexTestUtils {

    private AiIndexTestUtils() {}

    public static AiIndex randomAiIndex() {
        return new AiIndex(
            ESTestCase.randomBoolean() ? null : ESTestCase.randomAlphaOfLength(10),
            ESTestCase.randomBoolean() ? null : ESTestCase.randomAlphaOfLength(8),
            ESTestCase.randomBoolean() ? null : ESTestCase.randomAlphaOfLengthBetween(1, 40),
            ESTestCase.randomBoolean(),
            ESTestCase.randomBoolean() ? null : ESTestCase.randomBoolean(),
            ESTestCase.randomBoolean() ? null : randomTimestamp(),
            ESTestCase.randomBoolean() ? null : randomTimestamp(),
            ESTestCase.randomBoolean() ? null : randomDest(),
            ESTestCase.randomList(0, 3, () -> new TypeValue("workflow", ESTestCase.randomAlphaOfLength(8))),
            ESTestCase.randomList(0, 3, () -> new TypeValue(ESTestCase.randomFrom("esql", "connector"), ESTestCase.randomAlphaOfLength(8))),
            ESTestCase.randomList(
                0,
                3,
                () -> new TypeValue(ESTestCase.randomFrom("elastic_agent", "index", "esql"), ESTestCase.randomAlphaOfLength(8))
            ),
            ESTestCase.randomBoolean() ? null : Map.of("enabled", ESTestCase.randomBoolean(), "agent_id", ESTestCase.randomAlphaOfLength(6))
        );
    }

    /** A record that passes {@code PutAiIndexAction.Request} validation for the given id. */
    public static AiIndex validAiIndex(String id) {
        return new AiIndex(
            ESTestCase.randomBoolean() ? null : id,
            ESTestCase.randomAlphaOfLength(8),
            ESTestCase.randomBoolean() ? null : ESTestCase.randomAlphaOfLengthBetween(1, 40),
            ESTestCase.randomBoolean(),
            ESTestCase.randomBoolean() ? null : ESTestCase.randomBoolean(),
            null,
            null,
            randomDest(),
            List.of(),
            ESTestCase.randomList(0, 2, () -> new TypeValue("esql", ESTestCase.randomAlphaOfLength(8))),
            List.of(),
            null
        );
    }

    public static TypeValue randomDest() {
        return new TypeValue(
            ESTestCase.randomFrom(AiIndex.DEST_TYPE_INDEX, AiIndex.DEST_TYPE_DATA_STREAM),
            "ai-index-idx-" + ESTestCase.randomAlphaOfLength(6)
        );
    }

    public static Instant randomTimestamp() {
        return Instant.ofEpochMilli(ESTestCase.randomLongBetween(0, 4_102_444_800_000L));
    }
}
