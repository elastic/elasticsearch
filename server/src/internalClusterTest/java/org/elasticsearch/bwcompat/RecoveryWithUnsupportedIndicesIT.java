/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.bwcompat;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.CorruptStateException;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

@LuceneTestCase.SuppressCodecs("*")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, minNumDataNodes = 0, maxNumDataNodes = 0)
public class RecoveryWithUnsupportedIndicesIT extends ESIntegTestCase {

    /**
     * Return settings that could be used to start a node that has the given zipped home directory.
     */
    private Settings prepareBackwardsDataDir(Path indexDir, Path backwardsIndex) throws IOException {
        Path dataDir = indexDir.resolve("data");
        try (InputStream stream = Files.newInputStream(backwardsIndex)) {
            TestUtil.unzip(stream, indexDir);
        }
        assertTrue(Files.exists(dataDir));

        // list clusters in the datapath, ignoring anything from extrasfs
        final Path[] list;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir)) {
            List<Path> dirs = new ArrayList<>();
            for (Path p : stream) {
                if (p.getFileName().toString().startsWith("extra") == false) {
                    dirs.add(p);
                }
            }
            list = dirs.toArray(new Path[0]);
        }

        if (list.length != 1) {
            StringBuilder builder = new StringBuilder("Backwards index must contain exactly one cluster\n");
            for (Path line : list) {
                builder.append(line.toString()).append('\n');
            }
            throw new IllegalStateException(builder.toString());
        }
        Path src = list[0].resolve("nodes");
        Path dest = dataDir.resolve("nodes");
        assertTrue(Files.exists(src));
        Files.move(src, dest);
        assertFalse(Files.exists(src));
        assertTrue(Files.exists(dest));
        Settings.Builder builder = Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), dataDir.toAbsolutePath());

        return builder.build();
    }

    public void testUpgradeStartClusterOn_2_4_5() throws Exception {
        String indexName = "unsupported-2.4.5";

        Path indexDir = createTempDir();
        logger.info("Checking static index {}", indexName);
        Settings nodeSettings = prepareBackwardsDataDir(indexDir, getDataPath("/indices/bwc").resolve(indexName + ".zip"));
        assertThat(
            ExceptionsHelper.unwrap(
                expectThrows(Exception.class, () -> internalCluster().startNode(nodeSettings)),
                CorruptStateException.class
            ).getMessage(),
            containsString("Format version is not supported")
        );
    }
}
