/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
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
import static org.hamcrest.Matchers.nullValue;

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

    public void testByteBufferSliceMmap() throws Exception {
        int regions = randomIntBetween(1, 4);
        int regionSize = randomIntBetween(1, 16) * SharedBytes.PAGE_SIZE;
        var nodeSettings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), "node")
            .put("path.home", createTempDir())
            .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toString())
            .build();
        SharedBytes sharedBytes = null;
        try (var nodeEnv = new NodeEnvironment(nodeSettings, TestEnvironment.newEnvironment(nodeSettings))) {
            // mmap=true
            sharedBytes = new SharedBytes(regions, regionSize, nodeEnv, ignored -> {}, ignored -> {}, true);
            int region = randomIntBetween(0, regions - 1);
            byte[] randomData = randomByteArrayOfLength(regionSize);
            ByteBuffer tempBuffer = ByteBuffer.allocate(regionSize);
            SharedBytes.copyToCacheFileAligned(
                sharedBytes.getFileChannel(region),
                new ByteArrayInputStream(randomData),
                0,
                writtenBytesCount -> {},
                tempBuffer
            );

            SharedBytes.IO io = sharedBytes.getFileChannel(region);

            // byteBufferSlice returns a non-null read-only buffer with correct data
            int sliceOffset = randomIntBetween(0, regionSize / 2);
            int sliceLength = randomIntBetween(1, regionSize - sliceOffset);
            ByteBuffer slice = io.byteBufferSlice(sliceOffset, sliceLength);
            assertNotNull(slice);
            assertTrue(slice.isReadOnly());
            assertEquals(sliceLength, slice.remaining());
            byte[] sliceData = new byte[sliceLength];
            slice.get(sliceData);
            for (int i = 0; i < sliceLength; i++) {
                assertEquals(randomData[sliceOffset + i], sliceData[i]);
            }
        } finally {
            if (sharedBytes != null) {
                sharedBytes.decRef();
            }
        }
    }

    public void testByteBufferSliceNoMmap() throws Exception {
        int regions = randomIntBetween(1, 4);
        int regionSize = randomIntBetween(1, 16) * SharedBytes.PAGE_SIZE;
        var nodeSettings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), "node")
            .put("path.home", createTempDir())
            .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toString())
            .build();
        SharedBytes sharedBytes = null;
        try (var nodeEnv = new NodeEnvironment(nodeSettings, TestEnvironment.newEnvironment(nodeSettings))) {
            // mmap=false
            sharedBytes = new SharedBytes(regions, regionSize, nodeEnv, ignored -> {}, ignored -> {}, false);
            int region = randomIntBetween(0, regions - 1);
            SharedBytes.IO io = sharedBytes.getFileChannel(region);

            // byteBufferSlice returns null when not mmap'd
            assertThat(io.byteBufferSlice(0, regionSize), nullValue());
        } finally {
            if (sharedBytes != null) {
                sharedBytes.decRef();
            }
        }
    }

    public void testByteBufferSliceBoundsCheck() throws Exception {
        int regions = 1;
        int regionSize = 4 * SharedBytes.PAGE_SIZE;
        var nodeSettings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), "node")
            .put("path.home", createTempDir())
            .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toString())
            .build();
        SharedBytes sharedBytes = null;
        try (var nodeEnv = new NodeEnvironment(nodeSettings, TestEnvironment.newEnvironment(nodeSettings))) {
            sharedBytes = new SharedBytes(regions, regionSize, nodeEnv, ignored -> {}, ignored -> {}, true);
            SharedBytes.IO io = sharedBytes.getFileChannel(0);

            var expectedType = Assertions.ENABLED ? AssertionError.class : IllegalArgumentException.class;
            // position + length exceeds region size
            expectThrows(expectedType, () -> io.byteBufferSlice(regionSize - 10, 20));
            // negative position
            expectThrows(expectedType, () -> io.byteBufferSlice(-1, 10));
        } finally {
            if (sharedBytes != null) {
                sharedBytes.decRef();
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
