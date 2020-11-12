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
package org.elasticsearch.indices;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.nio.file.Path;
import java.util.Map;

public class DiskShardsBalanceTests extends ESSingleNodeTestCase {

    public IndicesService getIndicesService() {
        return getInstanceFromNode(IndicesService.class);
    }

    public NodeEnvironment getNodeEnvironment() {
        return getInstanceFromNode(NodeEnvironment.class);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    // config 3 datapaths
    private int DataPathCount = 3;

    @Override
    protected Settings nodeSettings() {
        final String[] absPaths = new String[DataPathCount];
        for (int i = 0; i < 3; i++) {
            absPaths[i] = createTempDir().toAbsolutePath().toString();
        }
        Settings settings = Settings.builder()
            .putList(Environment.PATH_DATA_SETTING.getKey(), absPaths).build();
        return settings;
    }

    public void testDiskShardsBalance() {
        final Settings threeShardsSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        final Settings oneShardSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        IndicesService indicesService = getIndicesService();
        Path[] paths = getNodeEnvironment().nodeDataPaths();

        Map<Path, Integer> actualDataPathToShardCount = null;
        Map<Path, Integer> expectedDataPathToShardCount = null;

        // create index1 with 3 shards and expect all data path' shard count is 1
        createIndex("index1", threeShardsSettings);
        actualDataPathToShardCount = indicesService.dataPathToShardCount();
        assertTrue(actualDataPathToShardCount.size() == 3);
        expectedDataPathToShardCount = Map.of(paths[0], 1, paths[1], 1, paths[2], 1);
        for (Map.Entry<Path, Integer> entry : expectedDataPathToShardCount.entrySet()) {
            assertTrue(entry.getValue() == actualDataPathToShardCount.get(entry.getKey()));
        }

        // create index2 with 3 shards and expect all data path' shards is 2
        createIndex("index2", threeShardsSettings);
        actualDataPathToShardCount = indicesService.dataPathToShardCount();
        assertTrue(actualDataPathToShardCount.size() == 3);
        expectedDataPathToShardCount = Map.of(paths[0], 2, paths[1], 2, paths[2], 2);
        for (Map.Entry<Path, Integer> entry : expectedDataPathToShardCount.entrySet()) {
            assertTrue(entry.getValue() == actualDataPathToShardCount.get(entry.getKey()));
        }

        // create index3_1 with 1 shards and expect datapath[0] is 3
        createIndex("index3_1", oneShardSettings);
        actualDataPathToShardCount = indicesService.dataPathToShardCount();
        assertTrue(actualDataPathToShardCount.size() == 3);
        expectedDataPathToShardCount = Map.of(paths[0], 3, paths[1], 2, paths[2], 2);
        for (Map.Entry<Path, Integer> entry : expectedDataPathToShardCount.entrySet()) {
            assertTrue(entry.getValue() == actualDataPathToShardCount.get(entry.getKey()));
        }

        // create index3_2 with 1 shards and expect datapath[1] is 3
        createIndex("index3_2", oneShardSettings);
        actualDataPathToShardCount = indicesService.dataPathToShardCount();
        assertTrue(actualDataPathToShardCount.size() == 3);
        expectedDataPathToShardCount = Map.of(paths[0], 3, paths[1], 3, paths[2], 2);
        for (Map.Entry<Path, Integer> entry : expectedDataPathToShardCount.entrySet()) {
            assertTrue(entry.getValue() == actualDataPathToShardCount.get(entry.getKey()));
        }

        // create index3_3 with 1 shards and expect datapath[2] is 3
        createIndex("index3_3", oneShardSettings);
        actualDataPathToShardCount = indicesService.dataPathToShardCount();
        assertTrue(actualDataPathToShardCount.size() == 3);
        expectedDataPathToShardCount = Map.of(paths[0], 3, paths[1], 3, paths[2], 3);
        for (Map.Entry<Path, Integer> entry : expectedDataPathToShardCount.entrySet()) {
            assertTrue(entry.getValue() == actualDataPathToShardCount.get(entry.getKey()));
        }

    }


}
