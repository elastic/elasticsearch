/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

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
                randomIntBetween(1, 16) * 4096,
                nodeEnv,
                ignored -> {},
                ignored -> {},
                IOUtils.WINDOWS == false && randomBoolean()
            );
            final var sharedBytesPath = nodeEnv.nodeDataPaths()[0].resolve("shared_snapshot_cache");
            assertTrue(Files.exists(sharedBytesPath));
            sharedBytes.decRef();
            assertFalse(Files.exists(sharedBytesPath));
        }
    }

    public void testCopyAllWith0Padding() throws Exception {
        int regions = randomIntBetween(1, 4);
        int regionSize = randomIntBetween(1, 16) * SharedBytes.PAGE_SIZE;
        ByteBuffer tempBuffer;
        if (randomBoolean()) {
            tempBuffer = ByteBuffer.allocate(randomIntBetween(1, 8) * SharedBytes.PAGE_SIZE);
        } else {
            tempBuffer = ByteBuffer.allocateDirect(randomIntBetween(1, 8) * SharedBytes.PAGE_SIZE);
        }
        var nodeSettings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), "node")
            .put("path.home", createTempDir())
            .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toString())
            .build();
        SharedBytes sharedBytes = null;
        try (var nodeEnv = new NodeEnvironment(nodeSettings, TestEnvironment.newEnvironment(nodeSettings))) {
            sharedBytes = new SharedBytes(
                regions,
                regionSize,
                nodeEnv,
                ignored -> {},
                ignored -> {},
                IOUtils.WINDOWS == false && randomBoolean()
            );
            assertNotNull(sharedBytes);
            int region = randomIntBetween(0, regions - 1);
            byte[] fullRegionRandomData = randomByteArrayOfLength(regionSize);
            // write full region
            {
                int bytesWritten = SharedBytes.copyToCacheFileAligned(
                    sharedBytes.getFileChannel(region),
                    new TestSlowByteArrayInputStream(fullRegionRandomData),
                    0,
                    writtenBytesCount -> {},
                    tempBuffer
                );
                assertThat(bytesWritten, equalTo(fullRegionRandomData.length));
                // read back region and verify whole region is written correctly
                byte[] readRegionData = new byte[regionSize];
                sharedBytes.getFileChannel(region).read(ByteBuffer.wrap(readRegionData), 0);
                assertArrayEquals(fullRegionRandomData, readRegionData);
            }
            // now write less than a full region
            byte[] randomData = randomByteArrayOfLength(randomIntBetween(1, regionSize - 1));
            // position must always be a multiple of PAGE_SIZE
            int position = (randomIntBetween(0, regionSize - randomData.length) / SharedBytes.PAGE_SIZE) * SharedBytes.PAGE_SIZE;
            {
                int bytesWritten = SharedBytes.copyToCacheFileAligned(
                    sharedBytes.getFileChannel(region),
                    new TestSlowByteArrayInputStream(randomData),
                    position,
                    writtenBytesCount -> {},
                    tempBuffer
                );
                assertThat(bytesWritten % SharedBytes.PAGE_SIZE, equalTo(0));
                assertThat(bytesWritten, greaterThanOrEqualTo(randomData.length));
                // read back region and verify region is written starting from position and padded with 0
                byte[] readRegionData = new byte[bytesWritten];
                sharedBytes.getFileChannel(region).read(ByteBuffer.wrap(readRegionData), position);
                for (int i = 0; i < randomData.length; i++) {
                    assertEquals(randomData[i], readRegionData[i]);
                }
                // assert padding is 0
                for (int i = randomData.length; i < readRegionData.length; i++) {
                    assertEquals((byte) 0, readRegionData[i]);
                }
            }
        } finally {
            if (sharedBytes != null) {
                sharedBytes.decRef();
            }
        }
    }

    /**
     * Test that mmap'd SharedBytes instances release their mapped memory on close, so that the OS
     * can reclaim disk space immediately. Without proper unmapping, each iteration leaks the cache
     * file's data blocks (the file is unlinked but the mapping holds the blocks allocated).
     *
     * On Linux, this is verified deterministically by inspecting {@code /proc/self/maps} for
     * residual memory-mapped regions after all instances are closed. A leaked (unmapped but not
     * munmap'd) file would still appear as a "(deleted)" entry in the maps file.
     */
    public void testMmapResourcesReleasedOnClose() throws Exception {
        assumeTrue("test relies on /proc/self/maps to verify mmap cleanup", IOUtils.LINUX);

        int regions = 4;
        int regionSize = 1024 * 1024; // 1 MB per region
        int iterations = 100;

        var dataPath = createTempDir();
        var nodeSettings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), "node")
            .put("path.home", createTempDir())
            .putList(Environment.PATH_DATA_SETTING.getKey(), dataPath.toString())
            .build();

        try (var nodeEnv = new NodeEnvironment(nodeSettings, TestEnvironment.newEnvironment(nodeSettings))) {
            var cachePath = nodeEnv.nodeDataPaths()[0].resolve("shared_snapshot_cache");

            // Warm up: create and close once to stabilise filesystem state
            new SharedBytes(regions, regionSize, nodeEnv, ignored -> {}, ignored -> {}, true).decRef();
            assertFalse(Files.exists(cachePath));

            for (int i = 0; i < iterations; i++) {
                SharedBytes sharedBytes = new SharedBytes(regions, regionSize, nodeEnv, ignored -> {}, ignored -> {}, true);
                assertTrue("cache file should exist", Files.exists(cachePath));
                sharedBytes.decRef();
                assertFalse("cache file should be deleted after close", Files.exists(cachePath));
            }

            // Verify that no mmap'd regions for the cache file remain after close.
            // If mmap buffers were not properly unmapped, the deleted cache file would still
            // appear as a "(deleted)" entry in /proc/self/maps, and the kernel would continue
            // to hold the file's disk blocks allocated until the mapping is released.
            try (var lines = Files.lines(PathUtils.get("/proc/self/maps"))) {
                var leakedMappings = lines.filter(line -> line.contains("shared_snapshot_cache")).toList();
                assertEquals(
                    "Found leaked memory-mapped regions for shared_snapshot_cache in /proc/self/maps after closing "
                        + iterations
                        + " mmap'd SharedBytes instances. "
                        + "This indicates that mmap buffers are not being properly unmapped on close. "
                        + "Leaked entries: "
                        + leakedMappings,
                    0,
                    leakedMappings.size()
                );
            }
        }
    }

    private static class TestSlowByteArrayInputStream extends ByteArrayInputStream {

        private TestSlowByteArrayInputStream(byte[] data) {
            super(data);
        }

        @Override
        public int read(byte[] b, int off, int len) {
            // reads less in one go than the vanilla ByteArrayInputStream would otherwise read
            return super.read(b, off, randomIntBetween(0, len));
        }
    }
}
