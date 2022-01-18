/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import static org.elasticsearch.repositories.blobstore.testkit.RandomBlobContent.BUFFER_SIZE;
import static org.hamcrest.Matchers.equalTo;

public class RandomBlobContentBytesReferenceTests extends ESTestCase {

    public void testStreamInput() throws IOException {
        final AtomicBoolean readComplete = new AtomicBoolean();
        final RandomBlobContent randomBlobContent = new RandomBlobContent(
            "repo",
            randomLong(),
            () -> false,
            () -> assertTrue("multiple notifications", readComplete.compareAndSet(false, true))
        );

        final int length = randomSize();
        final int checksumStart = between(0, length - 1);
        final int checksumEnd = between(checksumStart, length);

        final byte[] output = new RandomBlobContentBytesReference(randomBlobContent, length).streamInput().readAllBytes();

        assertThat(output.length, equalTo(length));
        assertTrue(readComplete.get());

        for (int i = BUFFER_SIZE; i < output.length; i++) {
            assertThat("output repeats at position " + i, output[i], equalTo(output[i % BUFFER_SIZE]));
        }

        final CRC32 crc32 = new CRC32();
        crc32.update(output, checksumStart, checksumEnd - checksumStart);
        assertThat("checksum computed correctly", randomBlobContent.getChecksum(checksumStart, checksumEnd), equalTo(crc32.getValue()));
    }

    private static int randomSize() {
        return between(1, 30000);
    }

}
