/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gateway;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;

public class PriorityComparatorTests extends ESTestCase {

    public void testPreferNewIndices() {
        RoutingNodes.UnassignedShards shards = new RoutingNodes.UnassignedShards(mock(RoutingNodes.class));
        List<ShardRouting> shardRoutings = Arrays.asList(
            TestShardRouting.newShardRouting("oldest", 0, null, null,
                randomBoolean(), ShardRoutingState.UNASSIGNED, new UnassignedInfo(randomFrom(UnassignedInfo.Reason.values()), "foobar")),
            TestShardRouting.newShardRouting("newest", 0, null, null,
                randomBoolean(), ShardRoutingState.UNASSIGNED, new UnassignedInfo(randomFrom(UnassignedInfo.Reason.values()), "foobar")));
        Collections.shuffle(shardRoutings, random());
        for (ShardRouting routing : shardRoutings) {
            shards.add(routing);
        }
        shards.sort(new PriorityComparator() {
            @Override
            protected IndexMetadata getMetadata(Index index) {
                Settings settings;
                if ("oldest".equals(index.getName())) {
                    settings = buildSettings(10, 1);
                } else if ("newest".equals(index.getName())) {
                    settings = buildSettings(100, 1);
                } else {
                    settings = Settings.EMPTY;
                }

                return IndexMetadata.builder(index.getName()).settings(settings).build();
            }
        });
        RoutingNodes.UnassignedShards.UnassignedIterator iterator = shards.iterator();
        ShardRouting next = iterator.next();
        assertEquals("newest", next.getIndexName());
        next = iterator.next();
        assertEquals("oldest", next.getIndexName());
        assertFalse(iterator.hasNext());
    }

    public void testPreferPriorityIndices() {
        RoutingNodes.UnassignedShards shards = new RoutingNodes.UnassignedShards(mock(RoutingNodes.class));
        List<ShardRouting> shardRoutings = Arrays.asList(
            TestShardRouting.newShardRouting("oldest", 0, null, null,
                randomBoolean(), ShardRoutingState.UNASSIGNED, new UnassignedInfo(randomFrom(UnassignedInfo.Reason.values()), "foobar")),
            TestShardRouting.newShardRouting("newest", 0, null, null,
                randomBoolean(), ShardRoutingState.UNASSIGNED, new UnassignedInfo(randomFrom(UnassignedInfo.Reason.values()), "foobar")));
        Collections.shuffle(shardRoutings, random());
        for (ShardRouting routing : shardRoutings) {
            shards.add(routing);
        }
        shards.sort(new PriorityComparator() {
            @Override
            protected IndexMetadata getMetadata(Index index) {
                Settings settings;
                if ("oldest".equals(index.getName())) {
                    settings = buildSettings(10, 100);
                } else if ("newest".equals(index.getName())) {
                    settings = buildSettings(100, 1);
                } else {
                    settings = Settings.EMPTY;
                }

                return IndexMetadata.builder(index.getName()).settings(settings).build();
            }
        });
        RoutingNodes.UnassignedShards.UnassignedIterator iterator = shards.iterator();
        ShardRouting next = iterator.next();
        assertEquals("oldest", next.getIndexName());
        next = iterator.next();
        assertEquals("newest", next.getIndexName());
        assertFalse(iterator.hasNext());
    }

    public void testPreferSystemIndices() {
        RoutingNodes.UnassignedShards shards = new RoutingNodes.UnassignedShards(mock(RoutingNodes.class));
        List<ShardRouting> shardRoutings = Arrays.asList(
            TestShardRouting.newShardRouting("oldest", 0, null, null,
                randomBoolean(), ShardRoutingState.UNASSIGNED, new UnassignedInfo(randomFrom(UnassignedInfo.Reason.values()), "foobar")),
            TestShardRouting.newShardRouting("newest", 0, null, null,
                randomBoolean(), ShardRoutingState.UNASSIGNED, new UnassignedInfo(randomFrom(UnassignedInfo.Reason.values()), "foobar")));
        Collections.shuffle(shardRoutings, random());
        for (ShardRouting routing : shardRoutings) {
            shards.add(routing);
        }
        shards.sort(new PriorityComparator() {
            @Override
            protected IndexMetadata getMetadata(Index index) {
                Settings settings;
                boolean isSystem = false;
                if ("oldest".equals(index.getName())) {
                    settings = buildSettings(10, 100);
                    isSystem = true;
                } else if ("newest".equals(index.getName())) {
                    settings = buildSettings(100, 1);
                } else {
                    settings = Settings.EMPTY;
                }

                return IndexMetadata.builder(index.getName()).system(isSystem).settings(settings).build();
            }
        });
        RoutingNodes.UnassignedShards.UnassignedIterator iterator = shards.iterator();
        ShardRouting next = iterator.next();
        assertEquals("oldest", next.getIndexName());
        next = iterator.next();
        assertEquals("newest", next.getIndexName());
        assertFalse(iterator.hasNext());
    }

    public void testPriorityComparatorSort() {
        RoutingNodes.UnassignedShards shards = new RoutingNodes.UnassignedShards(mock(RoutingNodes.class));
        int numIndices = randomIntBetween(3, 99);
        IndexMetadata[] indices = new IndexMetadata[numIndices];
        final Map<String, IndexMetadata> map = new HashMap<>();

        for (int i = 0; i < indices.length; i++) {
            int priority = 0;
            int creationDate = 0;
            boolean isSystem = false;

            if (frequently()) {
                priority = randomIntBetween(1, 1000);
                creationDate = randomIntBetween(1, 10000);
            }
            if (rarely()) {
                isSystem = true;
            }
            // else sometimes just use the defaults

            indices[i] = IndexMetadata.builder(String.format(Locale.ROOT, "idx_%04d", i))
                .system(isSystem)
                .settings(buildSettings(creationDate, priority))
                .build();

            map.put(indices[i].getIndex().getName(), indices[i]);
        }
        int numShards = randomIntBetween(10, 100);
        for (int i = 0; i < numShards; i++) {
            IndexMetadata indexMeta = randomFrom(indices);
            shards.add(TestShardRouting.newShardRouting(indexMeta.getIndex().getName(), randomIntBetween(1, 5), null, null,
                    randomBoolean(), ShardRoutingState.UNASSIGNED, new UnassignedInfo(randomFrom(UnassignedInfo.Reason.values()),
                    "foobar")));
        }
        shards.sort(new PriorityComparator() {
            @Override
            protected IndexMetadata getMetadata(Index index) {
                return map.get(index.getName());
            }
        });
        ShardRouting previous = null;
        for (ShardRouting routing : shards) {
            if (previous != null) {
                IndexMetadata prevMeta = map.get(previous.getIndexName());
                IndexMetadata currentMeta = map.get(routing.getIndexName());

                if (prevMeta.isSystem() == currentMeta.isSystem()) {
                    final int prevPriority = prevMeta.getSettings().getAsInt(IndexMetadata.SETTING_PRIORITY, -1);
                    final int currentPriority = currentMeta.getSettings().getAsInt(IndexMetadata.SETTING_PRIORITY, -1);

                    if (prevPriority == currentPriority) {
                        final int prevCreationDate = prevMeta.getSettings().getAsInt(IndexMetadata.SETTING_CREATION_DATE, -1);
                        final int currentCreationDate = currentMeta.getSettings().getAsInt(IndexMetadata.SETTING_CREATION_DATE, -1);

                        if (prevCreationDate == currentCreationDate) {
                            final String prevName = prevMeta.getIndex().getName();
                            final String currentName = currentMeta.getIndex().getName();

                            if (prevName.equals(currentName) == false) {
                                assertThat(
                                    "indexName mismatch, expected:" + currentName + " after " + prevName,
                                    prevName,
                                    greaterThan(currentName)
                                );
                            }
                        } else {
                            assertThat(
                                "creationDate mismatch, expected:" + currentCreationDate + " after " + prevCreationDate,
                                prevCreationDate, greaterThan(currentCreationDate)
                            );
                        }
                    } else {
                        assertThat(
                            "priority mismatch, expected:" + currentPriority + " after " + prevPriority,
                            prevPriority, greaterThan(currentPriority)
                        );
                    }
                } else {
                    assertThat(
                        "system mismatch, expected:" + currentMeta.isSystem() + " after " + prevMeta.isSystem(),
                        prevMeta.isSystem(),
                        greaterThan(currentMeta.isSystem())
                    );
                }
            }
            previous = routing;
        }
    }

    private static Settings buildSettings(int creationDate, int priority) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_CREATION_DATE, creationDate)
            .put(IndexMetadata.SETTING_PRIORITY, priority)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
    }
}
