/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.lucene.store.ESIndexInputTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xpack.searchablesnapshots.cache.TestUtils.assertCounter;
import static org.elasticsearch.xpack.searchablesnapshots.cache.TestUtils.createCacheService;
import static org.elasticsearch.xpack.searchablesnapshots.cache.TestUtils.numberOfRanges;
import static org.elasticsearch.xpack.searchablesnapshots.cache.TestUtils.randomCacheRangeSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CacheBufferedIndexInputStatsTests extends ESIndexInputTestCase {

    private static final int MAX_FILE_LENGTH = 10_000;

    /**
     * These tests simulate the passage of time with a clock that advances 100ms each time it is read.
     */
    private static final long FAKE_CLOCK_ADVANCE_NANOS = TimeValue.timeValueMillis(100).nanos();

    public void testOpenCount() throws Exception {
        executeTestCase(createCacheService(random()),
            (fileName, fileContent, cacheDirectory) -> {
                try {
                    for (long i = 0L; i < randomLongBetween(1L, 20L); i++) {
                        IndexInputStats inputStats = cacheDirectory.getStats(fileName);
                        assertThat(inputStats, (i == 0L) ? nullValue() : notNullValue());

                        final IndexInput input = cacheDirectory.openInput(fileName, newIOContext(random()));
                        inputStats = cacheDirectory.getStats(fileName);
                        assertThat(inputStats.getOpened().longValue(), equalTo(i + 1L));
                        input.close();
                    }
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
    }

    public void testInnerOpenCount() throws Exception {
        final ByteSizeValue rangeSize = randomCacheRangeSize(random());
        final CacheService noEvictionCacheService = new CacheService(new ByteSizeValue(1, ByteSizeUnit.GB), rangeSize);

        executeTestCase(noEvictionCacheService,
            (fileName, fileContent, cacheDirectory) -> {
                try {
                    assertThat( cacheDirectory.getStats(fileName), nullValue());

                    final IndexInput input = cacheDirectory.openInput(fileName, newIOContext(random()));
                    for (int j = 0; j < input.length(); j++) {
                        input.readByte();
                    }
                    input.close();

                    final IndexInputStats inputStats = cacheDirectory.getStats(fileName);
                    assertThat("Inner IndexInput should have been opened for each cached range to write",
                        inputStats.getInnerOpened().longValue(), equalTo(numberOfRanges(input.length(), rangeSize.getBytes())));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
    }

    public void testCloseCount() throws Exception {
        executeTestCase(createCacheService(random()),
            (fileName, fileContent, cacheDirectory) -> {
                try {
                    for (long i = 0L; i < randomLongBetween(1L, 20L); i++) {
                        final IndexInput input = cacheDirectory.openInput(fileName, newIOContext(random()));

                        IndexInputStats inputStats = cacheDirectory.getStats(fileName);
                        assertThat(inputStats, notNullValue());

                        assertThat(inputStats.getClosed().longValue(), equalTo(i));
                        input.close();
                        assertThat(inputStats.getClosed().longValue(), equalTo(i + 1L));
                    }
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
    }

    public void testCachedBytesReadsAndWrites() throws Exception {
        // a cache service with a low range size but enough space to not evict the cache file
        final ByteSizeValue rangeSize = new ByteSizeValue(randomIntBetween(512, MAX_FILE_LENGTH), ByteSizeUnit.BYTES);
        final CacheService cacheService = new CacheService(new ByteSizeValue(1, ByteSizeUnit.GB), rangeSize);

        executeTestCase(cacheService, (fileName, fileContent, cacheDirectory) -> {
            try (IndexInput input = cacheDirectory.openInput(fileName, newIOContext(random()))) {
                final long length = input.length();

                IndexInputStats inputStats = cacheDirectory.getStats(fileName);
                assertThat(inputStats, notNullValue());

                randomReadAndSlice(input, Math.toIntExact(length));
                final long cachedBytesWriteCount = numberOfRanges(length, rangeSize.getBytes());

                assertThat(inputStats.getCachedBytesWritten(), notNullValue());
                assertThat(inputStats.getCachedBytesWritten().total(), equalTo(length));
                assertThat(inputStats.getCachedBytesWritten().count(), equalTo(cachedBytesWriteCount));
                assertThat(inputStats.getCachedBytesWritten().min(), greaterThan(0L));
                assertThat(inputStats.getCachedBytesWritten().max(),
                    (length < rangeSize.getBytes()) ? equalTo(length) : equalTo(rangeSize.getBytes()));
                assertThat(inputStats.getCachedBytesWritten().totalNanoseconds(),
                    equalTo(cachedBytesWriteCount * FAKE_CLOCK_ADVANCE_NANOS));

                assertThat(inputStats.getCachedBytesRead(), notNullValue());
                assertThat(inputStats.getCachedBytesRead().total(), greaterThanOrEqualTo(length));
                assertThat(inputStats.getCachedBytesRead().count(), greaterThan(0L));
                assertThat(inputStats.getCachedBytesRead().min(), greaterThan(0L));
                assertThat(inputStats.getCachedBytesRead().max(),
                    (length < rangeSize.getBytes()) ? lessThanOrEqualTo(length) : lessThanOrEqualTo(rangeSize.getBytes()));

                assertCounter(inputStats.getDirectBytesRead(), 0L, 0L, 0L, 0L);
                assertThat(inputStats.getDirectBytesRead().totalNanoseconds(), equalTo(0L));

            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    public void testDirectBytesReads() throws Exception {
        final CacheService noDiskSpaceLeftCacheService
            = new CacheService(new ByteSizeValue(0, ByteSizeUnit.BYTES), new ByteSizeValue(0, ByteSizeUnit.BYTES));

        executeTestCase(noDiskSpaceLeftCacheService, (fileName, fileContent, cacheDirectory) -> {
            assertThat(cacheDirectory.getStats(fileName), nullValue());
            final IOContext ioContext = newIOContext(random());

            try (IndexInput input = cacheDirectory.openInput(fileName, ioContext)) {
                final IndexInputStats inputStats = cacheDirectory.getStats(fileName);

                // account for internal buffered reads
                final long bufferSize = (long) BufferedIndexInput.bufferSize(ioContext);
                final long remaining = input.length() % bufferSize;
                final long expectedTotal = input.length();
                final long expectedCount = input.length() / bufferSize + (remaining > 0L ? 1L : 0L);
                final long minRead = remaining > 0L ? remaining : bufferSize;
                final long maxRead = input.length() < bufferSize ? input.length() : bufferSize;

                // read all index input sequentially as it simplifies testing
                final byte[] readBuffer = new byte[512];
                for (long i = 0L; i < input.length();) {
                    int size = between(1, Math.toIntExact(Math.min(readBuffer.length, input.length() - input.getFilePointer())));
                    input.readBytes(readBuffer, 0, size);
                    i += size;

                    // direct cache file reads are aligned with the internal buffer
                    long currentCount = i / bufferSize + (i % bufferSize > 0L ? 1L : 0L);
                    if (currentCount < expectedCount) {
                        assertCounter(inputStats.getDirectBytesRead(), currentCount * bufferSize, currentCount, bufferSize, bufferSize);
                    } else {
                        assertCounter(inputStats.getDirectBytesRead(), expectedTotal, expectedCount, minRead, maxRead);
                    }
                    assertThat(inputStats.getDirectBytesRead().totalNanoseconds(), equalTo(currentCount * FAKE_CLOCK_ADVANCE_NANOS));
                }

                // cache file has never been written nor read
                assertCounter(inputStats.getCachedBytesWritten(), 0L, 0L, 0L, 0L);
                assertCounter(inputStats.getCachedBytesRead(), 0L, 0L, 0L, 0L);
                assertThat(inputStats.getCachedBytesWritten().totalNanoseconds(), equalTo(0L));

            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    public void testReadBytesContiguously() throws Exception {
        // use default cache service settings
        final CacheService cacheService = new CacheService(Settings.EMPTY);

        executeTestCase(cacheService, (fileName, fileContent, cacheDirectory) -> {
            final IOContext ioContext = newIOContext(random());

            try (IndexInput input = cacheDirectory.openInput(fileName, ioContext)) {
                final IndexInputStats inputStats = cacheDirectory.getStats(fileName);

                // account for the CacheBufferedIndexInput internal buffer
                final long bufferSize = (long) BufferedIndexInput.bufferSize(ioContext);
                final long remaining = input.length() % bufferSize;
                final long expectedTotal = input.length();
                final long expectedCount = input.length() / bufferSize + (remaining > 0L ? 1L : 0L);
                final long minRead = remaining > 0L ? remaining : bufferSize;
                final long maxRead = input.length() < bufferSize ? input.length() : bufferSize;

                final byte[] readBuffer = new byte[512];

                // read the input input sequentially
                for (long bytesRead = 0L; bytesRead < input.length();) {
                    int size = between(1, Math.toIntExact(Math.min(readBuffer.length, input.length() - bytesRead)));
                    input.readBytes(readBuffer, 0, size);
                    bytesRead += size;

                    // cache file reads are aligned with internal buffered reads
                    long currentCount = bytesRead / bufferSize + (bytesRead % bufferSize > 0L ? 1L : 0L);
                    if (currentCount < expectedCount) {
                        assertCounter(inputStats.getContiguousReads(), currentCount * bufferSize, currentCount, bufferSize, bufferSize);
                        assertCounter(inputStats.getCachedBytesRead(), currentCount * bufferSize, currentCount, bufferSize, bufferSize);

                    } else {
                        assertCounter(inputStats.getContiguousReads(), expectedTotal, expectedCount, minRead, maxRead);
                        assertCounter(inputStats.getCachedBytesRead(), expectedTotal, expectedCount, minRead, maxRead);
                    }
                }

                // cache file has been written in a single chunk
                assertCounter(inputStats.getCachedBytesWritten(), input.length(), 1L, input.length(), input.length());
                assertThat(inputStats.getCachedBytesWritten().totalNanoseconds(), equalTo(FAKE_CLOCK_ADVANCE_NANOS));

                assertCounter(inputStats.getNonContiguousReads(), 0L, 0L, 0L, 0L);
                assertCounter(inputStats.getDirectBytesRead(), 0L, 0L, 0L, 0L);
                assertThat(inputStats.getDirectBytesRead().totalNanoseconds(), equalTo(0L));

            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    public void testReadBytesNonContiguously() throws Exception {
        // use default cache service settings
        final CacheService cacheService = new CacheService(Settings.EMPTY);

        executeTestCase(cacheService, (fileName, fileContent, cacheDirectory) -> {
            final IOContext ioContext = newIOContext(random());

            try (IndexInput input = cacheDirectory.openInput(fileName, ioContext)) {
                final IndexInputStats inputStats = cacheDirectory.getStats(fileName);

                long totalBytesRead = 0L;
                long minBytesRead = Long.MAX_VALUE;
                long maxBytesRead = Long.MIN_VALUE;

                for (long i = 1L; i <= randomLongBetween(1L, 10L); i++) {
                    final long randomPosition = randomLongBetween(1L, input.length() - 1L);
                    input.seek(randomPosition);

                    final byte[] readBuffer = new byte[512];
                    int size = between(1, Math.toIntExact(Math.min(readBuffer.length, input.length() - randomPosition)));
                    input.readBytes(readBuffer, 0, size);

                    // BufferedIndexInput tries to read as much bytes as possible
                    final long bytesRead = Math.min(BufferedIndexInput.bufferSize(ioContext), input.length() - randomPosition);
                    totalBytesRead += bytesRead;
                    minBytesRead = (bytesRead < minBytesRead) ? bytesRead : minBytesRead;
                    maxBytesRead = (bytesRead > maxBytesRead) ? bytesRead : maxBytesRead;

                    assertCounter(inputStats.getNonContiguousReads(), totalBytesRead, i, minBytesRead, maxBytesRead);

                    // seek to the beginning forces a refill of the internal buffer (and simplifies a lot the test)
                    input.seek(0L);
                }

                // cache file has been written in a single chunk
                assertCounter(inputStats.getCachedBytesWritten(), input.length(), 1L, input.length(), input.length());
                assertThat(inputStats.getCachedBytesWritten().totalNanoseconds(), equalTo(FAKE_CLOCK_ADVANCE_NANOS));

                assertCounter(inputStats.getContiguousReads(), 0L, 0L, 0L, 0L);
                assertCounter(inputStats.getDirectBytesRead(), 0L, 0L, 0L, 0L);
                assertThat(inputStats.getDirectBytesRead().totalNanoseconds(), equalTo(0L));

            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    public void testForwardSeeks() throws Exception {
        // use default cache service settings
        final CacheService cacheService = new CacheService(Settings.EMPTY);

        executeTestCase(cacheService, (fileName, fileContent, cacheDirectory) -> {
            final IOContext ioContext = newIOContext(random());
            try (IndexInput indexInput = cacheDirectory.openInput(fileName, ioContext)) {
                IndexInput input = indexInput;
                if (randomBoolean()) {
                    final long sliceOffset = randomLongBetween(0L, input.length() - 1L);
                    final long sliceLength = randomLongBetween(1L, input.length() - sliceOffset);
                    input = input.slice("slice", sliceOffset, sliceLength);
                }
                if (randomBoolean()) {
                    input = input.clone();
                }

                final IndexInputStats inputStats = cacheDirectory.getStats(fileName);
                final IndexInputStats.Counter forwardSmallSeeksCounter = inputStats.getForwardSmallSeeks();
                assertCounter(forwardSmallSeeksCounter, 0L, 0L, 0L, 0L);

                long totalSmallSeeks = 0L;
                long countSmallSeeks = 0L;
                long minSmallSeeks = Long.MAX_VALUE;
                long maxSmallSeeks = Long.MIN_VALUE;

                final IndexInputStats.Counter forwardLargeSeeksCounter = inputStats.getForwardLargeSeeks();
                assertCounter(forwardLargeSeeksCounter, 0L, 0L, 0L, 0L);

                long totalLargeSeeks = 0L;
                long countLargeSeeks = 0L;
                long minLargeSeeks = Long.MAX_VALUE;
                long maxLargeSeeks = Long.MIN_VALUE;

                while (input.getFilePointer() < input.length()) {
                    long moveForward = randomLongBetween(1L, input.length() - input.getFilePointer());
                    input.seek(input.getFilePointer() + moveForward);

                    if (inputStats.isLargeSeek(moveForward)) {
                        minLargeSeeks = (moveForward < minLargeSeeks) ? moveForward : minLargeSeeks;
                        maxLargeSeeks = (moveForward > maxLargeSeeks) ? moveForward : maxLargeSeeks;
                        totalLargeSeeks += moveForward;
                        countLargeSeeks += 1;

                        assertCounter(forwardLargeSeeksCounter, totalLargeSeeks, countLargeSeeks, minLargeSeeks, maxLargeSeeks);

                    } else {
                        minSmallSeeks = (moveForward < minSmallSeeks) ? moveForward : minSmallSeeks;
                        maxSmallSeeks = (moveForward > maxSmallSeeks) ? moveForward : maxSmallSeeks;
                        totalSmallSeeks += moveForward;
                        countSmallSeeks += 1;

                        assertCounter(forwardSmallSeeksCounter, totalSmallSeeks, countSmallSeeks, minSmallSeeks, maxSmallSeeks);
                    }
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    public void testBackwardSeeks() throws Exception {
        // use default cache service settings
        final CacheService cacheService = new CacheService(Settings.EMPTY);

        executeTestCase(cacheService, (fileName, fileContent, cacheDirectory) -> {
            final IOContext ioContext = newIOContext(random());
            try (IndexInput indexInput = cacheDirectory.openInput(fileName, ioContext)) {
                IndexInput input = indexInput;
                if (randomBoolean()) {
                    final long sliceOffset = randomLongBetween(0L, input.length() - 1L);
                    final long sliceLength = randomLongBetween(1L, input.length() - sliceOffset);
                    input = input.slice("slice", sliceOffset, sliceLength);
                }
                if (randomBoolean()) {
                    input = input.clone();
                }

                final IndexInputStats inputStats = cacheDirectory.getStats(fileName);
                final IndexInputStats.Counter backwardSmallSeeks = inputStats.getBackwardSmallSeeks();
                assertCounter(backwardSmallSeeks, 0L, 0L, 0L, 0L);

                long totalSmallSeeks = 0L;
                long countSmallSeeks = 0L;
                long minSmallSeeks = Long.MAX_VALUE;
                long maxSmallSeeks = Long.MIN_VALUE;

                final IndexInputStats.Counter backwardLargeSeeks = inputStats.getBackwardLargeSeeks();
                assertCounter(backwardLargeSeeks, 0L, 0L, 0L, 0L);

                long totalLargeSeeks = 0L;
                long countLargeSeeks = 0L;
                long minLargeSeeks = Long.MAX_VALUE;
                long maxLargeSeeks = Long.MIN_VALUE;

                input.seek(input.length());
                assertThat(input.getFilePointer(), equalTo(input.length()));

                do {
                    long moveBackward = -1L * randomLongBetween(1L, input.getFilePointer());
                    input.seek(input.getFilePointer() + moveBackward);

                    if (inputStats.isLargeSeek(moveBackward)) {
                        minLargeSeeks = (moveBackward < minLargeSeeks) ? moveBackward : minLargeSeeks;
                        maxLargeSeeks = (moveBackward > maxLargeSeeks) ? moveBackward : maxLargeSeeks;
                        totalLargeSeeks += moveBackward;
                        countLargeSeeks += 1;

                        assertCounter(backwardLargeSeeks, totalLargeSeeks, countLargeSeeks, minLargeSeeks, maxLargeSeeks);

                    } else {
                        minSmallSeeks = (moveBackward < minSmallSeeks) ? moveBackward : minSmallSeeks;
                        maxSmallSeeks = (moveBackward > maxSmallSeeks) ? moveBackward : maxSmallSeeks;
                        totalSmallSeeks += moveBackward;
                        countSmallSeeks += 1;

                        assertCounter(backwardSmallSeeks, totalSmallSeeks, countSmallSeeks, minSmallSeeks, maxSmallSeeks);
                    }

                } while (input.getFilePointer() > 0L);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    private static void executeTestCase(CacheService cacheService, TriConsumer<String, byte[], CacheDirectory> test) throws Exception {
        final byte[] fileContent = randomUnicodeOfLength(randomIntBetween(10, MAX_FILE_LENGTH)).getBytes(StandardCharsets.UTF_8);
        final String fileName = randomAlphaOfLength(10);
        final SnapshotId snapshotId = new SnapshotId("_name", "_uuid");
        final IndexId indexId = new IndexId("_name", "_uuid");
        final ShardId shardId = new ShardId("_name", "_uuid", 0);
        final AtomicLong fakeClock = new AtomicLong();

        final Long seekingThreshold = randomBoolean() ? randomLongBetween(1L, fileContent.length) : null;

        try (CacheService ignored = cacheService;
             Directory directory = newDirectory();
             CacheDirectory cacheDirectory =
                 new CacheDirectory(directory, cacheService, createTempDir(), snapshotId, indexId, shardId,
                     () -> fakeClock.addAndGet(FAKE_CLOCK_ADVANCE_NANOS)) {
                     @Override
                     IndexInputStats createIndexInputStats(long fileLength) {
                         if (seekingThreshold == null) {
                             return super.createIndexInputStats(fileLength);
                         }
                         return new IndexInputStats(fileLength, seekingThreshold);
                     }
                 }
        ) {
            cacheService.start();
            assertThat(cacheDirectory.getStats(fileName), nullValue());

            final IndexOutput indexOutput = directory.createOutput(fileName, newIOContext(random()));
            indexOutput.writeBytes(fileContent, fileContent.length);
            indexOutput.close();

            test.apply(fileName, fileContent, cacheDirectory);
        }
    }
}
