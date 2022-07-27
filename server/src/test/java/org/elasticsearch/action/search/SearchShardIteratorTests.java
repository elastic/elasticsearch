/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.OriginalIndicesTests;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.routing.GroupShardsIteratorTests;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class SearchShardIteratorTests extends ESTestCase {

    public void testShardId() {
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        SearchShardIterator searchShardIterator = new SearchShardIterator(null, shardId, Collections.emptyList(), OriginalIndices.NONE);
        assertSame(shardId, searchShardIterator.shardId());
    }

    public void testGetOriginalIndices() {
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        OriginalIndices originalIndices = new OriginalIndices(
            new String[] { randomAlphaOfLengthBetween(3, 10) },
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
        );
        SearchShardIterator searchShardIterator = new SearchShardIterator(null, shardId, Collections.emptyList(), originalIndices);
        assertSame(originalIndices, searchShardIterator.getOriginalIndices());
    }

    public void testGetClusterAlias() {
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        SearchShardIterator searchShardIterator = new SearchShardIterator(
            clusterAlias,
            shardId,
            Collections.emptyList(),
            OriginalIndices.NONE
        );
        assertEquals(clusterAlias, searchShardIterator.getClusterAlias());
    }

    public void testNewSearchShardTarget() {
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        OriginalIndices originalIndices = new OriginalIndices(
            new String[] { randomAlphaOfLengthBetween(3, 10) },
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
        );

        String nodeId = randomAlphaOfLengthBetween(3, 10);
        SearchShardIterator searchShardIterator = new SearchShardIterator(
            clusterAlias,
            shardId,
            List.of(nodeId),
            originalIndices,
            null,
            null
        );
        final SearchShardTarget searchShardTarget = searchShardIterator.nextOrNull();
        assertNotNull(searchShardTarget);
        assertThat(searchShardTarget.getNodeId(), equalTo(nodeId));
        assertEquals(clusterAlias, searchShardTarget.getClusterAlias());
        assertSame(shardId, searchShardTarget.getShardId());
        assertEquals(nodeId, searchShardTarget.getNodeId());
    }

    public void testEqualsAndHashcode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            randomSearchShardIterator(),
            s -> new SearchShardIterator(
                s.getClusterAlias(),
                s.shardId(),
                s.getTargetNodeIds(),
                s.getOriginalIndices(),
                s.getSearchContextId(),
                s.getSearchContextKeepAlive()
            ),
            s -> {
                if (randomBoolean()) {
                    String clusterAlias;
                    if (s.getClusterAlias() == null) {
                        clusterAlias = randomAlphaOfLengthBetween(5, 10);
                    } else {
                        clusterAlias = randomBoolean() ? null : s.getClusterAlias() + randomAlphaOfLength(3);
                    }
                    return new SearchShardIterator(
                        clusterAlias,
                        s.shardId(),
                        s.getTargetNodeIds(),
                        s.getOriginalIndices(),
                        s.getSearchContextId(),
                        s.getSearchContextKeepAlive()
                    );
                } else {
                    ShardId shardId = new ShardId(
                        randomAlphaOfLengthBetween(5, 10),
                        randomAlphaOfLength(10),
                        randomIntBetween(0, Integer.MAX_VALUE)
                    );
                    return new SearchShardIterator(
                        s.getClusterAlias(),
                        shardId,
                        s.getTargetNodeIds(),
                        s.getOriginalIndices(),
                        s.getSearchContextId(),
                        s.getSearchContextKeepAlive()
                    );
                }
            }
        );
    }

    public void testCompareTo() {
        String[] clusters = generateRandomStringArray(2, 10, false, false);
        Arrays.sort(clusters);
        String[] indices = generateRandomStringArray(3, 10, false, false);
        Arrays.sort(indices);
        String[] uuids = generateRandomStringArray(3, 10, false, false);
        Arrays.sort(uuids);
        List<SearchShardIterator> shardIterators = new ArrayList<>();
        int numShards = randomIntBetween(1, 5);
        for (int i = 0; i < numShards; i++) {
            for (String index : indices) {
                for (String uuid : uuids) {
                    ShardId shardId = new ShardId(index, uuid, i);
                    shardIterators.add(
                        new SearchShardIterator(
                            null,
                            shardId,
                            GroupShardsIteratorTests.randomShardRoutings(shardId),
                            OriginalIndicesTests.randomOriginalIndices()
                        )
                    );
                    for (String cluster : clusters) {
                        shardIterators.add(
                            new SearchShardIterator(
                                cluster,
                                shardId,
                                GroupShardsIteratorTests.randomShardRoutings(shardId),
                                OriginalIndicesTests.randomOriginalIndices()
                            )
                        );
                    }

                }
            }
        }
        for (int i = 0; i < shardIterators.size(); i++) {
            SearchShardIterator currentIterator = shardIterators.get(i);
            for (int j = i + 1; j < shardIterators.size(); j++) {
                SearchShardIterator greaterIterator = shardIterators.get(j);
                assertThat(currentIterator, Matchers.lessThan(greaterIterator));
                assertThat(greaterIterator, Matchers.greaterThan(currentIterator));
                assertNotEquals(currentIterator, greaterIterator);
            }
            for (int j = i - 1; j >= 0; j--) {
                SearchShardIterator smallerIterator = shardIterators.get(j);
                assertThat(smallerIterator, Matchers.lessThan(currentIterator));
                assertThat(currentIterator, Matchers.greaterThan(smallerIterator));
                assertNotEquals(currentIterator, smallerIterator);
            }
        }
    }

    public void testCompareToEqualItems() {
        SearchShardIterator shardIterator1 = randomSearchShardIterator();
        SearchShardIterator shardIterator2 = new SearchShardIterator(
            shardIterator1.getClusterAlias(),
            shardIterator1.shardId(),
            shardIterator1.getTargetNodeIds(),
            shardIterator1.getOriginalIndices(),
            shardIterator1.getSearchContextId(),
            shardIterator1.getSearchContextKeepAlive()
        );
        assertEquals(shardIterator1, shardIterator2);
        assertEquals(0, shardIterator1.compareTo(shardIterator2));
        assertEquals(0, shardIterator2.compareTo(shardIterator1));
    }

    private static SearchShardIterator randomSearchShardIterator() {
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomIntBetween(0, Integer.MAX_VALUE));
        return new SearchShardIterator(
            clusterAlias,
            shardId,
            GroupShardsIteratorTests.randomShardRoutings(shardId),
            OriginalIndicesTests.randomOriginalIndices()
        );
    }
}
