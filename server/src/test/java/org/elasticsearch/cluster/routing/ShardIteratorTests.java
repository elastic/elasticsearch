/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.action.search.SearchShardIteratorTests;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardIdTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ShardIteratorTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            randomPlainShardIterator(),
            i -> new ShardIterator(i.shardId(), i.getShardRoutings()),
            i -> new ShardIterator(ShardIdTests.mutate(i.shardId()), i.getShardRoutings())
        );
    }

    public void testCompareTo() {
        String[] indices = generateRandomStringArray(3, 10, false, false);
        Arrays.sort(indices);
        String[] uuids = generateRandomStringArray(3, 10, false, false);
        Arrays.sort(uuids);
        List<ShardIterator> shardIterators = new ArrayList<>();
        int numShards = randomIntBetween(1, 5);
        for (int i = 0; i < numShards; i++) {
            for (String index : indices) {
                for (String uuid : uuids) {
                    ShardId shardId = new ShardId(index, uuid, i);
                    shardIterators.add(new ShardIterator(shardId, SearchShardIteratorTests.randomShardRoutings(shardId)));
                }
            }
        }
        for (int i = 0; i < shardIterators.size(); i++) {
            ShardIterator currentIterator = shardIterators.get(i);
            for (int j = i + 1; j < shardIterators.size(); j++) {
                ShardIterator greaterIterator = shardIterators.get(j);
                assertThat(currentIterator, Matchers.lessThan(greaterIterator));
                assertThat(greaterIterator, Matchers.greaterThan(currentIterator));
                // ShardId equality does not consider the index name
                if (currentIterator.shardId().id() != greaterIterator.shardId().id()
                    || currentIterator.shardId().getIndex().getUUID().equals(greaterIterator.shardId().getIndex().getUUID()) == false) {
                    assertNotEquals(currentIterator, greaterIterator);
                }
            }
            for (int j = i - 1; j >= 0; j--) {
                ShardIterator smallerIterator = shardIterators.get(j);
                assertThat(smallerIterator, Matchers.lessThan(currentIterator));
                assertThat(currentIterator, Matchers.greaterThan(smallerIterator));
                // ShardId equality does not consider the index name
                if (currentIterator.shardId().id() != smallerIterator.shardId().id()
                    || currentIterator.shardId().getIndex().getUUID().equals(smallerIterator.shardId().getIndex().getUUID()) == false) {
                    assertNotEquals(currentIterator, smallerIterator);
                }
            }
        }
    }

    public void testCompareToEqualItems() {
        ShardIterator shardIterator1 = randomPlainShardIterator();
        ShardIterator shardIterator2 = new ShardIterator(shardIterator1.shardId(), shardIterator1.getShardRoutings());
        assertEquals(shardIterator1, shardIterator2);
        assertEquals(0, shardIterator1.compareTo(shardIterator2));
        assertEquals(0, shardIterator2.compareTo(shardIterator1));
    }

    private static ShardIterator randomPlainShardIterator() {
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomIntBetween(1, Integer.MAX_VALUE));
        return new ShardIterator(shardId, SearchShardIteratorTests.randomShardRoutings(shardId));
    }
}
