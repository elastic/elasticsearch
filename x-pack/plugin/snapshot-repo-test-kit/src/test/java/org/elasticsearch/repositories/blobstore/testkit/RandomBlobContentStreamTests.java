/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import static org.elasticsearch.repositories.blobstore.testkit.RandomBlobContent.BUFFER_SIZE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class RandomBlobContentStreamTests extends ESTestCase {

    public void testReadAndResetAndSkip() {

        final byte[] output = new byte[randomSize()];
        final AtomicBoolean readComplete = new AtomicBoolean();
        int position = 0;
        int mark = 0;
        int resetCount = 0;

        final int checksumStart = between(0, output.length - 1);
        final int checksumEnd = between(checksumStart, output.length);
        final long checksum;

        final RandomBlobContent randomBlobContent = new RandomBlobContent(
            "repo",
            randomLong(),
            () -> false,
            () -> assertTrue("multiple notifications", readComplete.compareAndSet(false, true))
        );

        try (RandomBlobContentStream randomBlobContentStream = new RandomBlobContentStream(randomBlobContent, output.length)) {

            assertTrue(randomBlobContentStream.markSupported());

            checksum = randomBlobContent.getChecksum(checksumStart, checksumEnd);

            while (readComplete.get() == false) {
                switch (between(1, resetCount < 10 ? 4 : 3)) {
                    case 1:
                        randomBlobContentStream.mark(between(0, Integer.MAX_VALUE));
                        mark = position;
                        break;
                    case 2:
                        final int nextByte = randomBlobContentStream.read();
                        assertThat(nextByte, not(equalTo(-1)));
                        output[position++] = (byte) nextByte;
                        break;
                    case 3:
                        final int len = between(0, output.length - position);
                        assertThat(randomBlobContentStream.read(output, position, len), equalTo(len));
                        position += len;
                        break;
                    case 4:
                        randomBlobContentStream.reset();
                        resetCount += 1;
                        if (randomBoolean()) {
                            final int skipBytes = between(0, position - mark);
                            assertThat(randomBlobContentStream.skip(skipBytes), equalTo((long) skipBytes));
                            position = mark + skipBytes;
                        } else {
                            position = mark;
                        }
                        break;
                }
            }

            if (randomBoolean()) {
                assertThat(randomBlobContentStream.read(), equalTo(-1));
            } else {
                assertThat(randomBlobContentStream.read(output, 0, between(0, output.length)), equalTo(-1));
            }
        }

        for (int i = BUFFER_SIZE; i < output.length; i++) {
            assertThat("output repeats at position " + i, output[i], equalTo(output[i % BUFFER_SIZE]));
        }

        final CRC32 crc32 = new CRC32();
        crc32.update(output, checksumStart, checksumEnd - checksumStart);
        assertThat("checksum computed correctly", checksum, equalTo(crc32.getValue()));
    }

    public void testNotifiesOfCompletionOnce() throws IOException {

        final AtomicBoolean readComplete = new AtomicBoolean();
        final RandomBlobContent randomBlobContent = new RandomBlobContent(
            "repo",
            randomLong(),
            () -> false,
            () -> assertTrue("multiple notifications", readComplete.compareAndSet(false, true))
        );

        try (RandomBlobContentStream randomBlobContentStream = new RandomBlobContentStream(randomBlobContent, randomSize())) {
            for (int i = between(0, 4); i > 0; i--) {
                randomBlobContentStream.reset();
                randomBlobContentStream.readAllBytes();
                assertTrue(readComplete.get());
            }
        }

        assertTrue(readComplete.get()); // even if not completely read
    }

    public void testReadingOneByteThrowsExceptionAfterCancellation() {
        final RandomBlobContent randomBlobContent = new RandomBlobContent("repo", randomLong(), () -> true, () -> {});

        try (RandomBlobContentStream randomBlobContentStream = new RandomBlobContentStream(randomBlobContent, randomSize())) {
            // noinspection ResultOfMethodCallIgnored
            expectThrows(RepositoryVerificationException.class, randomBlobContentStream::read);
        }
    }

    public void testReadingBytesThrowsExceptionAfterCancellation() {
        final RandomBlobContent randomBlobContent = new RandomBlobContent("repo", randomLong(), () -> true, () -> {});

        try (RandomBlobContentStream randomBlobContentStream = new RandomBlobContentStream(randomBlobContent, randomSize())) {
            expectThrows(RepositoryVerificationException.class, randomBlobContentStream::readAllBytes);
        }
    }

    private static int randomSize() {
        return between(1, 30000);
    }

}
