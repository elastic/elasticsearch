/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class RepositoryAnalyzeActionTests extends ESTestCase {

    public void testGetBlobSizesAssertions() {
        // Just checking that no assertions are thrown
        getBlobSizesOrException(randomLongBetween(1L, Long.MAX_VALUE), randomLongBetween(1L, Long.MAX_VALUE), between(100, 10000));
    }

    public void testGetBlobSizesAssertionsSmall() {
        // Just checking that no assertions are thrown for an exhaustive set of small examples
        for (long maxBlobSize = 1L; maxBlobSize < 50L; maxBlobSize++) {
            for (long maxTotalDataSize = 1L; maxTotalDataSize < 50L; maxTotalDataSize++) {
                for (int blobCount = 1; blobCount < 25; blobCount++) {
                    getBlobSizesOrException(maxBlobSize, maxTotalDataSize, blobCount);
                }
            }
        }
    }

    public void testGetBlobSizesWithoutRepeats() {
        final int logBlobSize = 10;
        final long blobSize = 1 << logBlobSize;
        final long totalSize = 2047;
        assertThat(
            getBlobSizes(blobSize, totalSize, logBlobSize + 1).stream().mapToLong(l -> l).sorted().toArray(),
            equalTo(new long[] { 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 }) // one of each size
        );
    }

    public void testGetBlobSizesWithRepeats() {
        final int logBlobSize = 10;
        final long blobSize = 1 << logBlobSize;
        final long totalSize = 6014; // large enough for repeats
        assertThat(
            getBlobSizes(blobSize, totalSize, 26).stream().mapToLong(l -> l).sorted().toArray(),
            equalTo(
                new long[] { 1, 1, 2, 2, 4, 4, 8, 8, 16, 16, 32, 32, 64, 64, 128, 128, 128, 256, 256, 256, 512, 512, 512, 1024, 1024, 1024 }
            )
        );
    }

    public void testGetBlobSizesTotalTooSmall() {
        final int logBlobSize = 10;
        final long blobSize = 1 << logBlobSize;
        final long totalSize = 2046; // not large enough for one of each size
        assertThat(
            getBlobSizes(blobSize, totalSize, logBlobSize + 1).stream().mapToLong(l -> l).sorted().toArray(),
            equalTo(new long[] { 2, 4, 8, 16, 32, 64, 128, 128, 256, 256, 1024 })
        );
    }

    public void testGetBlobSizesDefaultExample() {
        // The defaults are 10MB max blob size, 1GB max total size, 100 blobs, which gives 4 blobs of each size (either powers of 2 or 10MB)
        assertThat(
            RepositoryAnalyzeAction.getBlobSizes(new RepositoryAnalyzeAction.Request("repo")).stream().mapToLong(l -> l).sorted().toArray(),
            equalTo(
                LongStream.range(0, 25)
                    .flatMap(power -> IntStream.range(0, 4).mapToLong(i -> power == 24 ? ByteSizeValue.ofMb(10).getBytes() : 1L << power))
                    .toArray()
            )
        );
    }

    public void testGetBlobSizesRealisticExample() {
        // The docs suggest a realistic example needs 2GB max blob size, 1TB max total size, 2000 blobs, which gives 62 or 63 blobs of each
        // size (powers of 2 up to 2GB)
        assertThat(
            getBlobSizes(ByteSizeValue.ofGb(2).getBytes(), ByteSizeValue.ofTb(1).getBytes(), 2000).stream()
                .mapToLong(l -> l)
                .sorted()
                .toArray(),
            equalTo(
                LongStream.range(0, 32).flatMap(power -> IntStream.range(0, power < 16 ? 62 : 63).mapToLong(i -> 1L << power)).toArray()
            )
        );
    }

    private static void getBlobSizesOrException(long maxBlobSize, long maxTotalDataSize, int blobCount) {
        try {
            getBlobSizes(maxBlobSize, maxTotalDataSize, blobCount);
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    private static List<Long> getBlobSizes(long maxBlobSize, long maxTotalDataSize, int blobCount) {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("repo");
        request.maxBlobSize(ByteSizeValue.ofBytes(maxBlobSize));
        request.maxTotalDataSize(ByteSizeValue.ofBytes(maxTotalDataSize));
        request.blobCount(blobCount);
        return RepositoryAnalyzeAction.getBlobSizes(request);
    }

}
