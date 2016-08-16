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

package org.elasticsearch.test;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.upgrade.get.IndexUpgradeStatus;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.IndexFolderUpgrader;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.Segment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.junit.Assert.assertEquals;


public class OldIndexUtils {

    public static List<String> loadIndexesList(String prefix, Path bwcIndicesPath) throws IOException {
        List<String> indexes = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(bwcIndicesPath, prefix + "-*.zip")) {
            for (Path path : stream) {
                indexes.add(path.getFileName().toString());
            }
        }
        Collections.sort(indexes);
        return indexes;
    }

    public static Settings getSettings() {
        return Settings.builder()
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false) // disable merging so no segments will be upgraded
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 30) //
            // speed up recoveries
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 30)
            .build();
    }

    public static void upgradeIndexFolder(InternalTestCluster cluster, String nodeName) throws Exception {
        final NodeEnvironment nodeEnvironment = cluster.getInstance(NodeEnvironment.class, nodeName);
        IndexFolderUpgrader.upgradeIndicesIfNeeded(Settings.EMPTY, nodeEnvironment);
    }

    public static void loadIndex(String indexName, String indexFile, Path unzipDir, Path bwcPath, ESLogger logger, Path... paths) throws
        Exception {
        Path unzipDataDir = unzipDir.resolve("data");

        Path backwardsIndex = bwcPath.resolve(indexFile);
        // decompress the index
        try (InputStream stream = Files.newInputStream(backwardsIndex)) {
            TestUtil.unzip(stream, unzipDir);
        }

        // check it is unique
        assertTrue(Files.exists(unzipDataDir));
        Path[] list = FileSystemUtils.files(unzipDataDir);
        if (list.length != 1) {
            throw new IllegalStateException("Backwards index must contain exactly one cluster");
        }

        // the bwc scripts packs the indices under this path
        Path src = list[0].resolve("nodes/0/indices/" + indexName);
        assertTrue("[" + indexFile + "] missing index dir: " + src.toString(), Files.exists(src));
        copyIndex(logger, src, indexName, paths);
    }

    public static void assertNotUpgraded(Client client, String... index) throws Exception {
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            assertTrue("index " + status.getIndex() + " should not be zero sized", status.getTotalBytes() != 0);
            // TODO: it would be better for this to be strictly greater, but sometimes an extra flush
            // mysteriously happens after the second round of docs are indexed
            assertTrue("index " + status.getIndex() + " should have recovered some segments from transaction log",
                status.getTotalBytes() >= status.getToUpgradeBytes());
            assertTrue("index " + status.getIndex() + " should need upgrading", status.getToUpgradeBytes() != 0);
        }
    }

    @SuppressWarnings("unchecked")
    public static Collection<IndexUpgradeStatus> getUpgradeStatus(Client client, String... indices) throws Exception {
        UpgradeStatusResponse upgradeStatusResponse = client.admin().indices().prepareUpgradeStatus(indices).get();
        assertNoFailures(upgradeStatusResponse);
        return upgradeStatusResponse.getIndices().values();
    }

    // randomly distribute the files from src over dests paths
    public static void copyIndex(final ESLogger logger, final Path src, final String indexName, final Path... dests) throws IOException {
        Path destinationDataPath = dests[randomInt(dests.length - 1)];
        for (Path dest : dests) {
            Path indexDir = dest.resolve(indexName);
            assertFalse(Files.exists(indexDir));
            Files.createDirectories(indexDir);
        }
        Files.walkFileTree(src, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path relativeDir = src.relativize(dir);
                for (Path dest : dests) {
                    Path destDir = dest.resolve(indexName).resolve(relativeDir);
                    Files.createDirectories(destDir);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (file.getFileName().toString().equals(IndexWriter.WRITE_LOCK_NAME)) {
                    // skip lock file, we don't need it
                    logger.trace("Skipping lock file: {}", file);
                    return FileVisitResult.CONTINUE;
                }

                Path relativeFile = src.relativize(file);
                Path destFile = destinationDataPath.resolve(indexName).resolve(relativeFile);
                logger.trace("--> Moving {} to {}", relativeFile, destFile);
                Files.move(file, destFile);
                assertFalse(Files.exists(file));
                assertTrue(Files.exists(destFile));
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static void assertUpgraded(Client client, String... index) throws Exception {
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            assertTrue("index " + status.getIndex() + " should not be zero sized", status.getTotalBytes() != 0);
            assertEquals("index " + status.getIndex() + " should be upgraded",
                0, status.getToUpgradeBytes());
        }

        // double check using the segments api that all segments are actually upgraded
        IndicesSegmentResponse segsRsp;
        if (index == null) {
            segsRsp = client.admin().indices().prepareSegments().execute().actionGet();
        } else {
            segsRsp = client.admin().indices().prepareSegments(index).execute().actionGet();
        }
        for (IndexSegments indexSegments : segsRsp.getIndices().values()) {
            for (IndexShardSegments shard : indexSegments) {
                for (ShardSegments segs : shard.getShards()) {
                    for (Segment seg : segs.getSegments()) {
                        assertEquals("Index " + indexSegments.getIndex() + " has unupgraded segment " + seg.toString(),
                            Version.CURRENT.luceneVersion.major, seg.version.major);
                        assertEquals("Index " + indexSegments.getIndex() + " has unupgraded segment " + seg.toString(),
                            Version.CURRENT.luceneVersion.minor, seg.version.minor);
                    }
                }
            }
        }
    }

    public static boolean isUpgraded(Client client, String index) throws Exception {
        ESLogger logger = Loggers.getLogger(OldIndexUtils.class);
        int toUpgrade = 0;
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            logger.info("Index: {}, total: {}, toUpgrade: {}", status.getIndex(), status.getTotalBytes(), status.getToUpgradeBytes());
            toUpgrade += status.getToUpgradeBytes();
        }
        return toUpgrade == 0;
    }

    public static void assertUpgradeWorks(Client client, String indexName, Version version) throws Exception {
        if (OldIndexUtils.isLatestLuceneVersion(version) == false) {
            OldIndexUtils.assertNotUpgraded(client, indexName);
        }
        assertNoFailures(client.admin().indices().prepareUpgrade(indexName).get());
        assertUpgraded(client, indexName);
    }

    public static Version extractVersion(String index) {
        return Version.fromString(index.substring(index.indexOf('-') + 1, index.lastIndexOf('.')));
    }

    public static boolean isLatestLuceneVersion(Version version) {
        return version.luceneVersion.major == Version.CURRENT.luceneVersion.major &&
            version.luceneVersion.minor == Version.CURRENT.luceneVersion.minor;
    }
}
