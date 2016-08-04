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

package org.elasticsearch.common.util;

import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.shard.ShardStateMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.OldIndexUtils;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class IndexFolderUpgraderTests extends ESTestCase {

    /**
     * tests custom data paths are upgraded
     */
    public void testUpgradeCustomDataPath() throws IOException {
        Path customPath = createTempDir();
        final Settings nodeSettings = Settings.builder()
            .put(NodeEnvironment.ADD_NODE_LOCK_ID_TO_CUSTOM_PATH.getKey(), randomBoolean())
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), customPath.toAbsolutePath().toString()).build();
        try (NodeEnvironment nodeEnv = newNodeEnvironment(nodeSettings)) {
            final Index index = new Index(randomAsciiOfLength(10), UUIDs.randomBase64UUID());
            Settings settings = Settings.builder()
                .put(nodeSettings)
                .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_0_0)
                .put(IndexMetaData.SETTING_DATA_PATH, customPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5))
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build();
            IndexMetaData indexState = IndexMetaData.builder(index.getName()).settings(settings).build();
            int numIdxFiles = randomIntBetween(1, 5);
            int numTranslogFiles = randomIntBetween(1, 5);
            IndexSettings indexSettings = new IndexSettings(indexState, nodeSettings);
            writeIndex(nodeEnv, indexSettings, numIdxFiles, numTranslogFiles);
            IndexFolderUpgrader helper = new IndexFolderUpgrader(settings, nodeEnv);
            helper.upgrade(indexSettings.getIndex().getName());
            checkIndex(nodeEnv, indexSettings, numIdxFiles, numTranslogFiles);
        }
    }

    /**
     * tests upgrade on partially upgraded index, when we crash while upgrading
     */
    public void testPartialUpgradeCustomDataPath() throws IOException {
        Path customPath = createTempDir();
        final Settings nodeSettings = Settings.builder()
            .put(NodeEnvironment.ADD_NODE_LOCK_ID_TO_CUSTOM_PATH.getKey(), randomBoolean())
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), customPath.toAbsolutePath().toString()).build();
        try (NodeEnvironment nodeEnv = newNodeEnvironment(nodeSettings)) {
            final Index index = new Index(randomAsciiOfLength(10), UUIDs.randomBase64UUID());
            Settings settings = Settings.builder()
                .put(nodeSettings)
                .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_0_0)
                .put(IndexMetaData.SETTING_DATA_PATH, customPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5))
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build();
            IndexMetaData indexState = IndexMetaData.builder(index.getName()).settings(settings).build();
            int numIdxFiles = randomIntBetween(1, 5);
            int numTranslogFiles = randomIntBetween(1, 5);
            IndexSettings indexSettings = new IndexSettings(indexState, nodeSettings);
            writeIndex(nodeEnv, indexSettings, numIdxFiles, numTranslogFiles);
            IndexFolderUpgrader helper = new IndexFolderUpgrader(settings, nodeEnv) {
                @Override
                void upgrade(Index index, Path source, Path target) throws IOException {
                    if(randomBoolean()) {
                        throw new FileNotFoundException("simulated");
                    }
                }
            };
            // only upgrade some paths
            try {
                helper.upgrade(index.getName());
            } catch (IOException e) {
                assertTrue(e instanceof FileNotFoundException);
            }
            helper = new IndexFolderUpgrader(settings, nodeEnv);
            // try to upgrade again
            helper.upgrade(indexSettings.getIndex().getName());
            checkIndex(nodeEnv, indexSettings, numIdxFiles, numTranslogFiles);
        }
    }

    public void testUpgrade() throws IOException {
        final Settings nodeSettings = Settings.builder()
            .put(NodeEnvironment.ADD_NODE_LOCK_ID_TO_CUSTOM_PATH.getKey(), randomBoolean()).build();
        try (NodeEnvironment nodeEnv = newNodeEnvironment(nodeSettings)) {
            final Index index = new Index(randomAsciiOfLength(10), UUIDs.randomBase64UUID());
            Settings settings = Settings.builder()
                .put(nodeSettings)
                .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_0_0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5))
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build();
            IndexMetaData indexState = IndexMetaData.builder(index.getName()).settings(settings).build();
            int numIdxFiles = randomIntBetween(1, 5);
            int numTranslogFiles = randomIntBetween(1, 5);
            IndexSettings indexSettings = new IndexSettings(indexState, nodeSettings);
            writeIndex(nodeEnv, indexSettings, numIdxFiles, numTranslogFiles);
            IndexFolderUpgrader helper = new IndexFolderUpgrader(settings, nodeEnv);
            helper.upgrade(indexSettings.getIndex().getName());
            checkIndex(nodeEnv, indexSettings, numIdxFiles, numTranslogFiles);
        }
    }

    public void testUpgradeIndices() throws IOException {
        final Settings nodeSettings = Settings.builder()
            .put(NodeEnvironment.ADD_NODE_LOCK_ID_TO_CUSTOM_PATH.getKey(), randomBoolean()).build();
        try (NodeEnvironment nodeEnv = newNodeEnvironment(nodeSettings)) {
            Map<IndexSettings, Tuple<Integer, Integer>>  indexSettingsMap = new HashMap<>();
            for (int i = 0; i < randomIntBetween(2, 5); i++) {
                final Index index = new Index(randomAsciiOfLength(10), UUIDs.randomBase64UUID());
                Settings settings = Settings.builder()
                    .put(nodeSettings)
                    .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID())
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_0_0)
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5))
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build();
                IndexMetaData indexState = IndexMetaData.builder(index.getName()).settings(settings).build();
                Tuple<Integer, Integer> fileCounts = new Tuple<>(randomIntBetween(1, 5), randomIntBetween(1, 5));
                IndexSettings indexSettings = new IndexSettings(indexState, nodeSettings);
                indexSettingsMap.put(indexSettings, fileCounts);
                writeIndex(nodeEnv, indexSettings, fileCounts.v1(), fileCounts.v2());
            }
            IndexFolderUpgrader.upgradeIndicesIfNeeded(nodeSettings, nodeEnv);
            for (Map.Entry<IndexSettings, Tuple<Integer, Integer>> entry : indexSettingsMap.entrySet()) {
                checkIndex(nodeEnv, entry.getKey(), entry.getValue().v1(), entry.getValue().v2());
            }
        }
    }

    /**
     * Run upgrade on a real bwc index
     */
    public void testUpgradeRealIndex() throws IOException, URISyntaxException {
        List<Path> indexes = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(getBwcIndicesPath(), "index-*.zip")) {
            for (Path path : stream) {
                indexes.add(path);
            }
        }
        CollectionUtil.introSort(indexes, (o1, o2) -> o1.getFileName().compareTo(o2.getFileName()));
        final Path path = randomFrom(indexes);
        final String indexName = path.getFileName().toString().replace(".zip", "").toLowerCase(Locale.ROOT);
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment()) {
            Path unzipDir = createTempDir();
            Path unzipDataDir = unzipDir.resolve("data");
            // decompress the index
            try (InputStream stream = Files.newInputStream(path)) {
                TestUtil.unzip(stream, unzipDir);
            }
            // check it is unique
            assertTrue(Files.exists(unzipDataDir));
            Path[] list = FileSystemUtils.files(unzipDataDir);
            if (list.length != 1) {
                throw new IllegalStateException("Backwards index must contain exactly one cluster but was " + list.length);
            }
            // the bwc scripts packs the indices under this path
            Path src = list[0].resolve("nodes/0/indices/" + indexName);
            assertTrue("[" + path + "] missing index dir: " + src.toString(), Files.exists(src));
            final Path indicesPath = randomFrom(nodeEnvironment.nodePaths()).indicesPath;
            logger.info("--> injecting index [{}] into [{}]", indexName, indicesPath);
            OldIndexUtils.copyIndex(logger, src, indexName, indicesPath);
            IndexFolderUpgrader.upgradeIndicesIfNeeded(Settings.EMPTY, nodeEnvironment);

            // ensure old index folder is deleted
            Set<String> indexFolders = nodeEnvironment.availableIndexFolders();
            assertEquals(indexFolders.size(), 1);

            // ensure index metadata is moved
            IndexMetaData indexMetaData = IndexMetaData.FORMAT.loadLatestState(logger,
                nodeEnvironment.resolveIndexFolder(indexFolders.iterator().next()));
            assertNotNull(indexMetaData);
            Index index = indexMetaData.getIndex();
            assertEquals(index.getName(), indexName);

            Set<ShardId> shardIds = nodeEnvironment.findAllShardIds(index);
            // ensure all shards are moved
            assertEquals(shardIds.size(), indexMetaData.getNumberOfShards());
            for (ShardId shardId : shardIds) {
                final ShardPath shardPath = ShardPath.loadShardPath(logger, nodeEnvironment, shardId,
                    new IndexSettings(indexMetaData, Settings.EMPTY));
                final Path translog = shardPath.resolveTranslog();
                final Path idx = shardPath.resolveIndex();
                final Path state = shardPath.getShardStatePath().resolve(MetaDataStateFormat.STATE_DIR_NAME);
                assertTrue(shardPath.exists());
                assertTrue(Files.exists(translog));
                assertTrue(Files.exists(idx));
                assertTrue(Files.exists(state));
            }
        }
    }

    public void testNeedsUpgrade() throws IOException {
        final Index index = new Index("foo", UUIDs.randomBase64UUID());
        IndexMetaData indexState = IndexMetaData.builder(index.getName())
            .settings(Settings.builder()
                .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment()) {
            IndexMetaData.FORMAT.write(indexState, nodeEnvironment.indexPaths(index));
            assertFalse(IndexFolderUpgrader.needsUpgrade(index, index.getUUID()));
        }
    }

    private void checkIndex(NodeEnvironment nodeEnv, IndexSettings indexSettings,
                            int numIdxFiles, int numTranslogFiles) throws IOException {
        final Index index = indexSettings.getIndex();
        // ensure index state can be loaded
        IndexMetaData loadLatestState = IndexMetaData.FORMAT.loadLatestState(logger, nodeEnv.indexPaths(index));
        assertNotNull(loadLatestState);
        assertEquals(loadLatestState.getIndex(), index);
        for (int shardId = 0; shardId < indexSettings.getNumberOfShards(); shardId++) {
            // ensure shard path can be loaded
            ShardPath targetShardPath = ShardPath.loadShardPath(logger, nodeEnv, new ShardId(index, shardId), indexSettings);
            assertNotNull(targetShardPath);
            // ensure shard contents are copied over
            final Path translog = targetShardPath.resolveTranslog();
            final Path idx = targetShardPath.resolveIndex();

            // ensure index and translog files are copied over
            assertEquals(numTranslogFiles, FileSystemUtils.files(translog).length);
            assertEquals(numIdxFiles, FileSystemUtils.files(idx).length);
            Path[] files = FileSystemUtils.files(translog);
            final HashSet<Path> translogFiles = new HashSet<>(Arrays.asList(files));
            for (int i = 0; i < numTranslogFiles; i++) {
                final String name = Integer.toString(i);
                translogFiles.contains(translog.resolve(name + ".translog"));
                byte[] content = Files.readAllBytes(translog.resolve(name + ".translog"));
                assertEquals(name , new String(content, StandardCharsets.UTF_8));
            }
            Path[] indexFileList = FileSystemUtils.files(idx);
            final HashSet<Path> idxFiles = new HashSet<>(Arrays.asList(indexFileList));
            for (int i = 0; i < numIdxFiles; i++) {
                final String name = Integer.toString(i);
                idxFiles.contains(idx.resolve(name + ".tst"));
                byte[] content = Files.readAllBytes(idx.resolve(name + ".tst"));
                assertEquals(name, new String(content, StandardCharsets.UTF_8));
            }
        }
    }

    private void writeIndex(NodeEnvironment nodeEnv, IndexSettings indexSettings,
                            int numIdxFiles, int numTranslogFiles) throws IOException {
        NodeEnvironment.NodePath[] nodePaths = nodeEnv.nodePaths();
        Path[] oldIndexPaths = new Path[nodePaths.length];
        for (int i = 0; i < nodePaths.length; i++) {
            oldIndexPaths[i] = nodePaths[i].indicesPath.resolve(indexSettings.getIndex().getName());
        }
        IndexMetaData.FORMAT.write(indexSettings.getIndexMetaData(), oldIndexPaths);
        for (int id = 0; id < indexSettings.getNumberOfShards(); id++) {
            Path oldIndexPath = randomFrom(oldIndexPaths);
            ShardId shardId = new ShardId(indexSettings.getIndex(), id);
            if (indexSettings.hasCustomDataPath()) {
                Path customIndexPath = nodeEnv.resolveBaseCustomLocation(indexSettings).resolve(indexSettings.getIndex().getName());
                writeShard(shardId, customIndexPath, numIdxFiles, numTranslogFiles);
            } else {
                writeShard(shardId, oldIndexPath, numIdxFiles, numTranslogFiles);
            }
            ShardStateMetaData state = new ShardStateMetaData(true, indexSettings.getUUID(), AllocationId.newInitializing());
            ShardStateMetaData.FORMAT.write(state, oldIndexPath.resolve(String.valueOf(shardId.getId())));
        }
    }

    private void writeShard(ShardId shardId, Path indexLocation,
                            final int numIdxFiles, final int numTranslogFiles) throws IOException {
        Path oldShardDataPath = indexLocation.resolve(String.valueOf(shardId.getId()));
        final Path translogPath = oldShardDataPath.resolve(ShardPath.TRANSLOG_FOLDER_NAME);
        final Path idxPath = oldShardDataPath.resolve(ShardPath.INDEX_FOLDER_NAME);
        Files.createDirectories(translogPath);
        Files.createDirectories(idxPath);
        for (int i = 0; i < numIdxFiles; i++) {
            String filename = Integer.toString(i);
            try (BufferedWriter w = Files.newBufferedWriter(idxPath.resolve(filename + ".tst"),
                StandardCharsets.UTF_8)) {
                w.write(filename);
            }
        }
        for (int i = 0; i < numTranslogFiles; i++) {
            String filename = Integer.toString(i);
            try (BufferedWriter w = Files.newBufferedWriter(translogPath.resolve(filename + ".translog"),
                StandardCharsets.UTF_8)) {
                w.write(filename);
            }
        }
    }
}
