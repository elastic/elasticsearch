/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.junit.Assert.assertSame;

public class IndexLongFieldRangeTestUtils {

    static IndexLongFieldRange randomRange() {
        return switch (ESTestCase.between(1, 3)) {
            case 1 -> IndexLongFieldRange.UNKNOWN;
            case 2 -> IndexLongFieldRange.EMPTY;
            case 3 -> randomSpecificRange();
            default -> throw new AssertionError("impossible");
        };
    }

    static IndexLongFieldRange randomSpecificRange() {
        return randomSpecificRange(null);
    }

    static IndexLongFieldRange randomSpecificRange(Boolean complete) {
        IndexLongFieldRange range = IndexLongFieldRange.NO_SHARDS;

        final int shardCount = ESTestCase.between(1, 5);
        for (int i = 0; i < shardCount; i++) {
            if (Boolean.FALSE.equals(complete) && range.getShards().length == shardCount - 1) {
                // caller requested an incomplete range so we must skip the last shard
                break;
            } else if (Boolean.TRUE.equals(complete) || randomBoolean()) {
                range = range.extendWithShardRange(
                    i,
                    shardCount,
                    randomBoolean() ? ShardLongFieldRange.EMPTY : ShardLongFieldRangeWireTests.randomSpecificRange()
                );
            }
        }

        assert range != IndexLongFieldRange.UNKNOWN;
        assert complete == null || complete.equals(range.isComplete());
        return range;
    }

    static boolean checkForSameInstances(IndexLongFieldRange expected, IndexLongFieldRange actual) {
        final boolean expectSame = expected == IndexLongFieldRange.UNKNOWN
            || expected == IndexLongFieldRange.EMPTY
            || expected == IndexLongFieldRange.NO_SHARDS;
        if (expectSame) {
            assertSame(expected, actual);
        }
        return expectSame;
    }

}
