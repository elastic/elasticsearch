/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.inference.ChunkingStrategy;
import org.elasticsearch.test.ESTestCase;

public class ChunkingStrategyTests extends ESTestCase {

    public void testValidChunkingStrategy() {
        ChunkingStrategy expected = randomFrom(ChunkingStrategy.values());

        assertEquals(expected, ChunkingStrategy.fromString(expected.toString()));
    }

    public void testInvalidChunkingStrategy() {
        assertThrows(IllegalArgumentException.class, () -> ChunkingStrategy.fromString(randomAlphaOfLength(10)));
    }
}
