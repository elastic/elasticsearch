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

import static org.hamcrest.Matchers.equalTo;

public class RepositoryAnalyzeActionTests extends ESTestCase {

    public void testGetBlobSizesAssertions() {
        getBlobSizesOrException(randomLongBetween(1L, Long.MAX_VALUE), randomLongBetween(1L, Long.MAX_VALUE), between(100, 10000));
    }

    public void testGetBlobSizesAssertionsSmall() {
        for (long maxBlobSize = 1L; maxBlobSize < 50L; maxBlobSize++) {
            for (long maxTotalDataSize = 1L; maxTotalDataSize < 50L; maxTotalDataSize++) {
                for (int blobCount = 1; blobCount < 25; blobCount++) {
                    getBlobSizesOrException(maxBlobSize, maxTotalDataSize, blobCount);
                }
            }
        }
    }

    private void getBlobSizesOrException(long maxBlobSize, long maxTotalDataSize, int blobCount) {
        try {
            getBlobSizes(maxBlobSize, maxTotalDataSize, blobCount);
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testGetBlobSizesExample() {
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

    private static List<Long> getBlobSizes(long maxBlobSize, long maxTotalDataSize, int blobCount) {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("repo");
        request.maxBlobSize(new ByteSizeValue(maxBlobSize));
        request.maxTotalDataSize(new ByteSizeValue(maxTotalDataSize));
        request.blobCount(blobCount);
        return RepositoryAnalyzeAction.getBlobSizes(request);
    }

}
