/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

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

    public void testPopulationListenerIsCalledOnSuccess() throws IOException {
        doWithSharedBytes(sharedBytes -> {
            // position + length must be < region size
            final int position = randomIntBetween(0, sharedBytes.regionSize);
            final int alignedPosition = position - position % PAGE_SIZE;
            final int streamLength = randomIntBetween(1, sharedBytes.regionSize - alignedPosition);
            logger.info("Copying {} bytes to position {} (region size={})", streamLength, alignedPosition, sharedBytes.regionSize);
            final int[] bytesCopiedHolder = new int[1];
            final long[] copyTimeNanosHolder = new long[1];
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(randomByteArrayOfLength(streamLength))) {
                SharedBytes.copyToCacheFileAligned(
                    sharedBytes.getFileChannel(0),
                    inputStream,
                    alignedPosition,
                    p -> {},
                    ByteBuffer.allocate(randomIntBetween(1, 4) * PAGE_SIZE),
                    (bytesCopied, copyTimeNanos) -> {
                        bytesCopiedHolder[0] = bytesCopied;
                        copyTimeNanosHolder[0] = copyTimeNanos;
                    }
                );
                final int partialPage = streamLength % PAGE_SIZE;
                final int pageAlignedCopySize = partialPage == 0 ? streamLength : streamLength + PAGE_SIZE - partialPage;
                assertEquals(pageAlignedCopySize, bytesCopiedHolder[0]);
                assertThat(copyTimeNanosHolder[0], greaterThan(0L));
            }
        });
    }

    public void testPopulationListenerIsNotCalledForZeroLengthStream() throws IOException {
        doWithSharedBytes(sharedBytes -> {
            final BlobCachePopulationListener listener = mock(BlobCachePopulationListener.class);
            final int position = randomIntBetween(0, sharedBytes.regionSize);
            final int alignedPosition = position - position % PAGE_SIZE;
            logger.info("Copying nothing to position {} (region size={})", alignedPosition, sharedBytes.regionSize);
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[] {})) {
                SharedBytes.copyToCacheFileAligned(
                    sharedBytes.getFileChannel(0),
                    inputStream,
                    alignedPosition,
                    p -> {},
                    ByteBuffer.allocate(randomIntBetween(1, 4) * PAGE_SIZE),
                    listener
                );
                verifyNoInteractions(listener);
            }
        });
    }

    private <E extends Exception> void doWithSharedBytes(CheckedConsumer<SharedBytes, E> consumer) throws E, IOException {
        int regions = randomIntBetween(1, 10);
        var nodeSettings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), "node")
            .put("path.home", createTempDir())
            .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toString())
            .build();
        try (var nodeEnv = new NodeEnvironment(nodeSettings, TestEnvironment.newEnvironment(nodeSettings))) {
            SharedBytes sharedBytes = null;
            try {
                sharedBytes = new SharedBytes(
                    regions,
                    randomIntBetween(1, 16) * 4096,
                    nodeEnv,
                    ignored -> {},
                    ignored -> {},
                    IOUtils.WINDOWS == false && randomBoolean()
                );
                consumer.accept(sharedBytes);
            } finally {
                sharedBytes.decRef();
            }
        }
    }
}
