/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gateway;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.*;

public class PriorityComparatorTests extends ESTestCase {

    public void testPreferNewIndices() {
        RoutingNodes.UnassignedShards shards = new RoutingNodes.UnassignedShards((RoutingNodes) null);
        List<ShardRouting> shardRoutings = Arrays.asList(TestShardRouting.newShardRouting("oldest", 0, null, null, null,
                randomBoolean(), ShardRoutingState.UNASSIGNED, 0, new UnassignedInfo(randomFrom(UnassignedInfo.Reason.values()), "foobar")), TestShardRouting.newShardRouting("newest", 0, null, null, null,
                randomBoolean(), ShardRoutingState.UNASSIGNED, 0, new UnassignedInfo(randomFrom(UnassignedInfo.Reason.values()), "foobar")));
        Collections.shuffle(shardRoutings, random());
        for (ShardRouting routing : shardRoutings) {
            shards.add(routing);
        }
        shards.sort(new PriorityComparator() {
            @Override
            protected Settings getIndexSettings(String index) {
                if ("oldest".equals(index)) {
                    return Settings.builder().put(IndexMetaData.SETTING_CREATION_DATE, 10)
                            .put(IndexMetaData.SETTING_PRIORITY, 1).build();
                } else if ("newest".equals(index)) {
                    return Settings.builder().put(IndexMetaData.SETTING_CREATION_DATE, 100)
                            .put(IndexMetaData.SETTING_PRIORITY, 1).build();
                }
                return Settings.EMPTY;
            }
        });
        RoutingNodes.UnassignedShards.UnassignedIterator iterator = shards.iterator();
        ShardRouting next = iterator.next();
        assertEquals("newest", next.index());
        next = iterator.next();
        assertEquals("oldest", next.index());
        assertFalse(iterator.hasNext());
    }

    public void testPreferPriorityIndices() {
        RoutingNodes.UnassignedShards shards = new RoutingNodes.UnassignedShards((RoutingNodes) null);
        List<ShardRouting> shardRoutings = Arrays.asList(TestShardRouting.newShardRouting("oldest", 0, null, null, null,
                randomBoolean(), ShardRoutingState.UNASSIGNED, 0, new UnassignedInfo(randomFrom(UnassignedInfo.Reason.values()), "foobar")), TestShardRouting.newShardRouting("newest", 0, null, null, null,
                randomBoolean(), ShardRoutingState.UNASSIGNED, 0, new UnassignedInfo(randomFrom(UnassignedInfo.Reason.values()), "foobar")));
        Collections.shuffle(shardRoutings, random());
        for (ShardRouting routing : shardRoutings) {
            shards.add(routing);
        }
        shards.sort(new PriorityComparator() {
            @Override
            protected Settings getIndexSettings(String index) {
                if ("oldest".equals(index)) {
                    return Settings.builder().put(IndexMetaData.SETTING_CREATION_DATE, 10)
                            .put(IndexMetaData.SETTING_PRIORITY, 100).build();
                } else if ("newest".equals(index)) {
                    return Settings.builder().put(IndexMetaData.SETTING_CREATION_DATE, 100)
                            .put(IndexMetaData.SETTING_PRIORITY, 1).build();
                }
                return Settings.EMPTY;
            }
        });
        RoutingNodes.UnassignedShards.UnassignedIterator iterator = shards.iterator();
        ShardRouting next = iterator.next();
        assertEquals("oldest", next.index());
        next = iterator.next();
        assertEquals("newest", next.index());
        assertFalse(iterator.hasNext());
    }

    public void testPriorityComparatorSort() {
        RoutingNodes.UnassignedShards shards = new RoutingNodes.UnassignedShards((RoutingNodes) null);
        int numIndices = randomIntBetween(3, 99);
        IndexMeta[] indices = new IndexMeta[numIndices];
        final Map<String, IndexMeta> map = new HashMap<>();

        for (int i = 0; i < indices.length; i++) {
            if (frequently()) {
                indices[i] = new IndexMeta("idx_2015_04_" + String.format(Locale.ROOT, "%02d", i), randomIntBetween(1, 1000), randomIntBetween(1, 10000));
            } else { // sometimes just use defaults
                indices[i] = new IndexMeta("idx_2015_04_" +  String.format(Locale.ROOT, "%02d", i));
            }
            map.put(indices[i].name, indices[i]);
        }
        int numShards = randomIntBetween(10, 100);
        for (int i = 0; i < numShards; i++) {
            IndexMeta indexMeta = randomFrom(indices);
            shards.add(TestShardRouting.newShardRouting(indexMeta.name, randomIntBetween(1, 5), null, null, null,
                    randomBoolean(), ShardRoutingState.UNASSIGNED, randomIntBetween(0, 100), new UnassignedInfo(randomFrom(UnassignedInfo.Reason.values()), "foobar")));
        }
        shards.sort(new PriorityComparator() {
            @Override
            protected Settings getIndexSettings(String index) {
                IndexMeta indexMeta = map.get(index);
                return indexMeta.settings;
            }
        });
        ShardRouting previous = null;
        for (ShardRouting routing : shards) {
            if (previous != null) {
                IndexMeta prevMeta = map.get(previous.getIndex());
                IndexMeta currentMeta = map.get(routing.getIndex());
                if (prevMeta.priority == currentMeta.priority) {
                    if (prevMeta.creationDate == currentMeta.creationDate) {
                        if (prevMeta.name.equals(currentMeta.name) == false) {
                            assertTrue("indexName mismatch, expected:" + currentMeta.name + " after " + prevMeta.name + " " + prevMeta.name.compareTo(currentMeta.name), prevMeta.name.compareTo(currentMeta.name) > 0);
                        }
                    } else {
                        assertTrue("creationDate mismatch, expected:" + currentMeta.creationDate + " after " + prevMeta.creationDate, prevMeta.creationDate > currentMeta.creationDate);
                    }
                } else {
                    assertTrue("priority mismatch, expected:" +  currentMeta.priority + " after " + prevMeta.priority, prevMeta.priority > currentMeta.priority);
                }
            }
            previous = routing;
        }
    }

    private static class IndexMeta {
        final String name;
        final int priority;
        final long creationDate;
        final Settings settings;

        private IndexMeta(String name) { // default
            this.name = name;
            this.priority = 1;
            this.creationDate = -1;
            this.settings = Settings.EMPTY;
        }

        private IndexMeta(String name, int priority, long creationDate) {
            this.name = name;
            this.priority = priority;
            this.creationDate = creationDate;
            this.settings = Settings.builder().put(IndexMetaData.SETTING_CREATION_DATE, creationDate)
                    .put(IndexMetaData.SETTING_PRIORITY, priority).build();
        }
    }
}
