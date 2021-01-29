/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.junit.Assert.assertSame;

public class IndexLongFieldRangeTestUtils {

    static IndexLongFieldRange randomRange() {
        switch (ESTestCase.between(1, 3)) {
            case 1:
                return IndexLongFieldRange.UNKNOWN;
            case 2:
                return IndexLongFieldRange.EMPTY;
            case 3:
                return randomSpecificRange();
            default:
                throw new AssertionError("impossible");
        }
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
                        randomBoolean() ? ShardLongFieldRange.EMPTY : ShardLongFieldRangeWireTests.randomSpecificRange());
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
