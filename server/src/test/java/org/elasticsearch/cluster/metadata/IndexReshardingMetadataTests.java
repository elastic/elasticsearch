/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.metadata.IndexReshardingMetadata.SourceShardState;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata.TargetShardState;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class IndexReshardingMetadataTests extends ESTestCase {
    // test that we can drive a split through all valid state transitions in random order and terminate
    public void testSplit() {
        final var numShards = randomIntBetween(1, 10);
        final var multiple = randomIntBetween(2, 5);

        final var metadata = IndexReshardingMetadata.newSplitByMultiple(numShards, multiple);

        // starting state is as expected
        assert metadata.oldShardCount() == numShards;
        assert metadata.newShardCount() == numShards * multiple;
        for (int i = 0; i < numShards; i++) {
            assert metadata.getSourceShardState(i) == SourceShardState.SOURCE;
        }
        for (int i = numShards; i < numShards * multiple; i++) {
            assert metadata.getTargetShardState(i) == TargetShardState.CLONE;
        }

        // advance split state randomly and expect to terminate
        while (metadata.splitInProgress()) {
            // pick a shard at random and see if we can advance it
            int idx = randomIntBetween(0, numShards * multiple - 1);
            if (idx < numShards) {
                // can we advance source?
                var sourceState = metadata.getSourceShardState(idx);
                var nextState = randomFrom(SourceShardState.values());
                if (nextState.ordinal() == sourceState.ordinal() + 1) {
                    var targets = metadata.getTargetStatesFor(idx);
                    if (Arrays.stream(targets).allMatch(target -> target == TargetShardState.DONE)) {
                        metadata.setSourceShardState(idx, nextState);
                    } else {
                        assertThrows(AssertionError.class, () -> metadata.setSourceShardState(idx, nextState));
                    }
                } else {
                    assertThrows(AssertionError.class, () -> metadata.setSourceShardState(idx, nextState));
                }
            } else {
                // can we advance target?
                var targetState = metadata.getTargetShardState(idx);
                var nextState = randomFrom(TargetShardState.values());
                if (nextState.ordinal() == targetState.ordinal() + 1) {
                    metadata.setTargetShardState(idx, nextState);
                } else {
                    assertThrows(AssertionError.class, () -> metadata.setTargetShardState(idx, nextState));
                }
            }
        }

        for (int i = 0; i < numShards; i++) {
            assert metadata.getSourceShardState(i) == SourceShardState.DONE;
        }
        for (int i = numShards; i < numShards * multiple; i++) {
            assert metadata.getTargetShardState(i) == TargetShardState.DONE;
        }
    }
}
