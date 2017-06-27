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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.MergePolicyConfig;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;


public class OldIndexUtils {

    public static List<String> loadDataFilesList(String prefix, Path bwcIndicesPath) throws IOException {
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

    public static Path getIndexDir(
        final Logger logger,
        final String indexName,
        final String indexFile,
        final Path dataDir) throws IOException {
        final Version version = Version.fromString(indexName.substring("index-".length()));
        if (version.before(Version.V_5_0_0_alpha1)) {
            // the bwc scripts packs the indices under this path
            Path src = dataDir.resolve("nodes/0/indices/" + indexName);
            assertTrue("[" + indexFile + "] missing index dir: " + src.toString(), Files.exists(src));
            return src;
        } else {
            final List<Path> indexFolders = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir.resolve("0/indices"),
                (p) -> p.getFileName().toString().startsWith("extra") == false)) { // extra FS can break this...
                for (final Path path : stream) {
                    indexFolders.add(path);
                }
            }
            assertThat(indexFolders.toString(), indexFolders.size(), equalTo(1));
            final IndexMetaData indexMetaData = IndexMetaData.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY,
                indexFolders.get(0));
            assertNotNull(indexMetaData);
            assertThat(indexFolders.get(0).getFileName().toString(), equalTo(indexMetaData.getIndexUUID()));
            assertThat(indexMetaData.getCreationVersion(), equalTo(version));
            return indexFolders.get(0);
        }
    }

    // randomly distribute the files from src over dests paths
    public static void copyIndex(final Logger logger, final Path src, final String folderName, final Path... dests) throws IOException {
        Path destinationDataPath = dests[randomInt(dests.length - 1)];
        for (Path dest : dests) {
            Path indexDir = dest.resolve(folderName);
            assertFalse(Files.exists(indexDir));
            Files.createDirectories(indexDir);
        }
        Files.walkFileTree(src, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path relativeDir = src.relativize(dir);
                for (Path dest : dests) {
                    Path destDir = dest.resolve(folderName).resolve(relativeDir);
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
                Path destFile = destinationDataPath.resolve(folderName).resolve(relativeFile);
                logger.trace("--> Moving {} to {}", relativeFile, destFile);
                Files.move(file, destFile);
                assertFalse(Files.exists(file));
                assertTrue(Files.exists(destFile));
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
