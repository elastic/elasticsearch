/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.repositories.RepositoryVerificationException;

import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.zip.CRC32;

/**
 * A random ~8kB block of data that is arbitrarily repeated, for use as a dummy payload for testing a blob store repo.
 */
class RandomBlobContent {

    static final int BUFFER_SIZE = 8191; // nearly 8kB, but prime, so unlikely to be aligned with any other block sizes

    final byte[] buffer = new byte[BUFFER_SIZE];
    private final BooleanSupplier isCancelledSupplier;
    private final AtomicReference<Runnable> onLastRead;
    private final String repositoryName;

    /**
     * @param repositoryName The name of the repository being tested, for use in exception messages.
     * @param seed RNG seed to use for its contents.
     * @param isCancelledSupplier Predicate that causes reads to throw a {@link RepositoryVerificationException}, allowing for fast failure
     *                            on cancellation.
     * @param onLastRead Runs when a {@code read()} call returns the last byte of the file, or on {@code close()} if the file was not fully
     *                   read. Only runs once even if the last byte is read multiple times using {@code mark()} and {@code reset()}.
     */
    RandomBlobContent(String repositoryName, long seed, BooleanSupplier isCancelledSupplier, Runnable onLastRead) {
        this.repositoryName = repositoryName;
        this.isCancelledSupplier = isCancelledSupplier;
        this.onLastRead = new AtomicReference<>(onLastRead);
        new Random(seed).nextBytes(buffer);
    }

    long getChecksum(long checksumRangeStart, long checksumRangeEnd) {
        assert 0 <= checksumRangeStart && checksumRangeStart <= checksumRangeEnd;
        final CRC32 crc32 = new CRC32();

        final long startBlock = checksumRangeStart / buffer.length;
        final long endBlock = (checksumRangeEnd - 1) / buffer.length;

        if (startBlock == endBlock) {
            crc32.update(
                buffer,
                Math.toIntExact(checksumRangeStart % buffer.length),
                Math.toIntExact(checksumRangeEnd - checksumRangeStart)
            );
        } else {
            final int bufferStart = Math.toIntExact(checksumRangeStart % buffer.length);
            crc32.update(buffer, bufferStart, buffer.length - bufferStart);
            for (long block = startBlock + 1; block < endBlock; block++) {
                crc32.update(buffer);
            }
            crc32.update(buffer, 0, Math.toIntExact((checksumRangeEnd - 1) % buffer.length) + 1);
        }

        return crc32.getValue();
    }

    void ensureNotCancelled(final String position) {
        if (isCancelledSupplier.getAsBoolean()) {
            throw new RepositoryVerificationException(repositoryName, "blob upload cancelled at position [" + position + "]");
        }
    }

    void onLastRead() {
        final Runnable runnable = onLastRead.getAndSet(null);
        if (runnable != null) {
            runnable.run();
        }
    }

}
