/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class StageIdTests extends ESTestCase {

    public void testAllStageIdsAreUnique() {
        final Set<Byte> ids = new HashSet<>();
        for (final StageId stageId : StageId.values()) {
            assertTrue("Duplicate stage ID: " + stageId, ids.add(stageId.id));
        }
    }

    public void testRoundTripAllStageIds() {
        for (final StageId stageId : StageId.values()) {
            assertEquals(stageId, StageId.fromId(stageId.id));
        }
    }

    public void testFromIdUnknownThrows() {
        final Set<Byte> validIds = Arrays.stream(StageId.values()).map(s -> s.id).collect(Collectors.toSet());

        for (int i = 0; i < 50; i++) {
            final byte randomId = randomByte();
            if (validIds.contains(randomId)) {
                continue;
            }
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> StageId.fromId(randomId));
            assertTrue(e.getMessage().contains("Unknown stage ID"));
        }
    }
}
