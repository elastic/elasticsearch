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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.bwcompat.OldIndexBackwardsCompatibilityTests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.shard.ShardStateMetaData;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.*;

/**
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class MultiDataPathUpgraderTests extends ElasticsearchTestCase {

    public void testUpgradeRandomPaths() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment()) {
            final String uuid = Strings.base64UUID();
            final ShardId shardId = new ShardId("foo", 0);
            final Path[] shardDataPaths = nodeEnvironment.availableShardPaths(shardId);
            if (nodeEnvironment.nodeDataPaths().length == 1) {
                MultiDataPathUpgrader helper = new MultiDataPathUpgrader(nodeEnvironment);
                assertFalse(helper.needsUpgrading(shardId));
                return;
            }
            int numIdxFiles = 0;
            int numTranslogFiles = 0;
            int metaStateVersion = 0;
            for (Path shardPath : shardDataPaths) {
                final Path translog = shardPath.resolve(ShardPath.TRANSLOG_FOLDER_NAME);
                final Path idx = shardPath.resolve(ShardPath.INDEX_FOLDER_NAME);
                Files.createDirectories(translog);
                Files.createDirectories(idx);
                int numFiles = randomIntBetween(1, 10);
                for (int i = 0; i < numFiles; i++, numIdxFiles++) {
                    String filename = Integer.toString(numIdxFiles);
                    try (BufferedWriter w = Files.newBufferedWriter(idx.resolve(filename + ".tst"), Charsets.UTF_8)) {
                        w.write(filename);
                    }
                }
                numFiles = randomIntBetween(1, 10);
                for (int i = 0; i < numFiles; i++, numTranslogFiles++) {
                    String filename = Integer.toString(numTranslogFiles);
                    try (BufferedWriter w = Files.newBufferedWriter(translog.resolve(filename + ".translog"), Charsets.UTF_8)) {
                        w.write(filename);
                    }
                }
                ++metaStateVersion;
                ShardStateMetaData.FORMAT.write(new ShardStateMetaData(metaStateVersion, true, uuid), metaStateVersion, shardDataPaths);
            }
            final Path path = randomFrom(shardDataPaths);
            ShardPath targetPath = new ShardPath(path, path, uuid, new ShardId("foo", 0));
            MultiDataPathUpgrader helper = new MultiDataPathUpgrader(nodeEnvironment);
            helper.upgrade(shardId, targetPath);
            assertFalse(helper.needsUpgrading(shardId));
            if (shardDataPaths.length > 1) {
                for (Path shardPath : shardDataPaths) {
                    if (shardPath.equals(targetPath.getDataPath())) {
                        continue;
                    }
                    final Path translog = shardPath.resolve(ShardPath.TRANSLOG_FOLDER_NAME);
                    final Path idx = shardPath.resolve(ShardPath.INDEX_FOLDER_NAME);
                    final Path state = shardPath.resolve(MetaDataStateFormat.STATE_DIR_NAME);
                    assertFalse(Files.exists(translog));
                    assertFalse(Files.exists(idx));
                    assertFalse(Files.exists(state));
                    assertFalse(Files.exists(shardPath));
                }
            }

            final ShardStateMetaData stateMetaData = ShardStateMetaData.FORMAT.loadLatestState(logger, targetPath.getShardStatePath());
            assertEquals(metaStateVersion, stateMetaData.version);
            assertTrue(stateMetaData.primary);
            assertEquals(uuid, stateMetaData.indexUUID);
            final Path translog = targetPath.getDataPath().resolve(ShardPath.TRANSLOG_FOLDER_NAME);
            final Path idx = targetPath.getDataPath().resolve(ShardPath.INDEX_FOLDER_NAME);
            Files.deleteIfExists(idx.resolve("write.lock"));
            assertEquals(numTranslogFiles, FileSystemUtils.files(translog).length);
            assertEquals(numIdxFiles, FileSystemUtils.files(idx).length);
            final HashSet<Path> translogFiles = Sets.newHashSet(FileSystemUtils.files(translog));
            for (int i = 0; i < numTranslogFiles; i++) {
                final String name = Integer.toString(i);
                translogFiles.contains(translog.resolve(name + ".translog"));
                byte[] content = Files.readAllBytes(translog.resolve(name + ".translog"));
                assertEquals(name , new String(content, Charsets.UTF_8));
            }
            final HashSet<Path> idxFiles = Sets.newHashSet(FileSystemUtils.files(idx));
            for (int i = 0; i < numIdxFiles; i++) {
                final String name = Integer.toString(i);
                idxFiles.contains(idx.resolve(name + ".tst"));
                byte[] content = Files.readAllBytes(idx.resolve(name + ".tst"));
                assertEquals(name , new String(content, Charsets.UTF_8));
            }
        }
    }

    /**
     * Run upgrade on a real bwc index
     */
    public void testUpgradeRealIndex() throws IOException, URISyntaxException {
        List<Path> indexes = new ArrayList<>();
        Path dir = getDataPath("/" + OldIndexBackwardsCompatibilityTests.class.getPackage().getName().replace('.', '/')); // the files are in the same pkg as the OldIndexBackwardsCompatibilityTests test
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "index-*.zip")) {
            for (Path path : stream) {
                indexes.add(path);
            }
        }
        CollectionUtil.introSort(indexes, new Comparator<Path>() {
            @Override
            public int compare(Path o1, Path o2) {
                return o1.getFileName().compareTo(o2.getFileName());
            }
        });
        final ShardId shardId = new ShardId("test", 0);
        final Path path = randomFrom(indexes);
        final Path indexFile = path;
        final String indexName = indexFile.getFileName().toString().replace(".zip", "").toLowerCase(Locale.ROOT);
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment()) {
            if (nodeEnvironment.nodeDataPaths().length == 1) {
                MultiDataPathUpgrader helper = new MultiDataPathUpgrader(nodeEnvironment);
                assertFalse(helper.needsUpgrading(shardId));
                return;
            }
            Path unzipDir = createTempDir();
            Path unzipDataDir = unzipDir.resolve("data");
            // decompress the index
            try (InputStream stream = Files.newInputStream(indexFile)) {
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
            assertTrue("[" + indexFile + "] missing index dir: " + src.toString(), Files.exists(src));
            Path[] multiDataPath = new Path[nodeEnvironment.nodeDataPaths().length];
            int i = 0;
            for (NodeEnvironment.NodePath nodePath : nodeEnvironment.nodePaths()) {
                multiDataPath[i++] = nodePath.indicesPath;
            }
            logger.info("--> injecting index [{}] into multiple data paths", indexName);
            OldIndexBackwardsCompatibilityTests.copyIndex(logger, src, indexName, multiDataPath);
            final ShardPath shardPath = new ShardPath(nodeEnvironment.availableShardPaths(new ShardId(indexName, 0))[0], nodeEnvironment.availableShardPaths(new ShardId(indexName, 0))[0], IndexMetaData.INDEX_UUID_NA_VALUE, new ShardId(indexName, 0));

            logger.info("{}", FileSystemUtils.files(shardPath.resolveIndex()));

            MultiDataPathUpgrader helper = new MultiDataPathUpgrader(nodeEnvironment);
            helper.upgrade(new ShardId(indexName, 0), shardPath);
            helper.checkIndex(shardPath);
            assertFalse(helper.needsUpgrading(new ShardId(indexName, 0)));
        }
    }

    public void testNeedsUpgrade() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment()) {
            String uuid = Strings.randomBase64UUID();
            final ShardId shardId = new ShardId("foo", 0);
            ShardStateMetaData.FORMAT.write(new ShardStateMetaData(1, true, uuid), 1, nodeEnvironment.availableShardPaths(shardId));
            MultiDataPathUpgrader helper = new MultiDataPathUpgrader(nodeEnvironment);
            boolean multiDataPaths = nodeEnvironment.nodeDataPaths().length > 1;
            boolean needsUpgrading = helper.needsUpgrading(shardId);
            if (multiDataPaths) {
                assertTrue(needsUpgrading);
            } else {
                assertFalse(needsUpgrading);
            }
        }
    }

    public void testPickTargetShardPath() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment()) {
            final ShardId shard = new ShardId("foo", 0);
            final Path[] paths = nodeEnvironment.availableShardPaths(shard);
            if (paths.length == 1) {
                MultiDataPathUpgrader helper = new MultiDataPathUpgrader(nodeEnvironment);
                try {
                    helper.pickShardPath(new ShardId("foo", 0));
                    fail("one path needs no upgrading");
                } catch (IllegalStateException ex) {
                    // only one path
                }
            } else {
                final Map<Path, Tuple<Long, Long>> pathToSpace = new HashMap<>();
                final Path expectedPath;
                if (randomBoolean()) { // path with most of the file bytes
                    expectedPath = randomFrom(paths);
                    long[] used = new long[paths.length];
                    long sumSpaceUsed = 0;
                    for (int i = 0; i < used.length; i++) {
                        long spaceUsed = paths[i] == expectedPath ? randomIntBetween(101, 200) : randomIntBetween(10, 100);
                        sumSpaceUsed += spaceUsed;
                        used[i] = spaceUsed;
                    }
                    for (int i = 0; i < used.length; i++) {
                        long availalbe = randomIntBetween((int)(2*sumSpaceUsed-used[i]), 4 * (int)sumSpaceUsed);
                        pathToSpace.put(paths[i], new Tuple<>(availalbe, used[i]));
                    }
                } else { // path with largest available space
                    expectedPath = randomFrom(paths);
                    long[] used = new long[paths.length];
                    long sumSpaceUsed = 0;
                    for (int i = 0; i < used.length; i++) {
                        long spaceUsed = randomIntBetween(10, 100);
                        sumSpaceUsed += spaceUsed;
                        used[i] = spaceUsed;
                    }

                    for (int i = 0; i < used.length; i++) {
                        long availalbe = paths[i] == expectedPath ? randomIntBetween((int)(sumSpaceUsed), (int)(2*sumSpaceUsed)) : randomIntBetween(0, (int)(sumSpaceUsed) - 1) ;
                        pathToSpace.put(paths[i], new Tuple<>(availalbe, used[i]));
                    }

                }
                MultiDataPathUpgrader helper = new MultiDataPathUpgrader(nodeEnvironment) {
                    @Override
                    protected long getUsabelSpace(NodeEnvironment.NodePath path) throws IOException {
                        return pathToSpace.get(path.resolve(shard)).v1();
                    }

                    @Override
                    protected long getSpaceUsedByShard(Path path) throws IOException {
                        return  pathToSpace.get(path).v2();
                    }
                };
                String uuid = Strings.randomBase64UUID();
                ShardStateMetaData.FORMAT.write(new ShardStateMetaData(1, true, uuid), 1, paths);
                final ShardPath shardPath = helper.pickShardPath(new ShardId("foo", 0));
                assertEquals(expectedPath, shardPath.getDataPath());
                assertEquals(expectedPath, shardPath.getShardStatePath());
            }

            MultiDataPathUpgrader helper = new MultiDataPathUpgrader(nodeEnvironment) {
                @Override
                protected long getUsabelSpace(NodeEnvironment.NodePath path) throws IOException {
                    return randomIntBetween(0, 10);
                }

                @Override
                protected long getSpaceUsedByShard(Path path) throws IOException {
                    return randomIntBetween(11, 20);
                }
            };

            try {
                helper.pickShardPath(new ShardId("foo", 0));
                fail("not enough space");
            } catch (IllegalStateException ex) {
                // not enough space
            }
        }
    }
}
