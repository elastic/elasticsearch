/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Files;

public class SharedBytesTests extends ESTestCase {

    public void testReleasesFileCorrectly() throws Exception {
        int regions = randomIntBetween(1, 10);
        var nodeSettings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), "node")
            .put("path.home", createTempDir())
            .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toString())
            .build();
        try (var nodeEnv = new NodeEnvironment(nodeSettings, TestEnvironment.newEnvironment(nodeSettings))) {
            final SharedBytes sharedBytes = new SharedBytes(
                regions,
                randomIntBetween(1, 16) * 4096L,
                nodeEnv,
                ignored -> {},
                ignored -> {}
            );
            final var sharedBytesPath = nodeEnv.nodeDataPaths()[0].resolve("shared_snapshot_cache");
            assertTrue(Files.exists(sharedBytesPath));
            SharedBytes.IO fileChannel = sharedBytes.getFileChannel(randomInt(regions - 1));
            assertTrue(Files.exists(sharedBytesPath));
            if (randomBoolean()) {
                fileChannel.close();
                assertTrue(Files.exists(sharedBytesPath));
                sharedBytes.decRef();
            } else {
                sharedBytes.decRef();
                assertTrue(Files.exists(sharedBytesPath));
                fileChannel.close();
            }
            assertFalse(Files.exists(sharedBytesPath));
        }
    }
}
