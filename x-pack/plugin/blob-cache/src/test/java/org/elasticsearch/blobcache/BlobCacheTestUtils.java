/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache;

import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.mockfile.FilterPath;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.common.CacheFile;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtilsForTesting;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.synchronizedNavigableSet;
import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

public final class BlobCacheTestUtils {
    private BlobCacheTestUtils() {}

    public static SortedSet<ByteRange> randomPopulateAndReads(final CacheFile cacheFile) {
        return randomPopulateAndReads(cacheFile, (fileChannel, aLong, aLong2) -> {});
    }

    public static SortedSet<ByteRange> randomPopulateAndReads(CacheFile cacheFile, TriConsumer<FileChannel, Long, Long> consumer) {
        final SortedSet<ByteRange> ranges = synchronizedNavigableSet(new TreeSet<>());
        final List<Future<Integer>> futures = new ArrayList<>();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        for (int i = 0; i < between(0, 10); i++) {
            final long start = randomLongBetween(0L, Math.max(0L, cacheFile.getLength() - 1L));
            final long end = randomLongBetween(Math.min(start + 1L, cacheFile.getLength()), cacheFile.getLength());
            final ByteRange range = ByteRange.of(start, end);
            futures.add(
                cacheFile.populateAndRead(range, range, channel -> Math.toIntExact(end - start), (channel, from, to, progressUpdater) -> {
                    consumer.apply(channel, from, to);
                    ranges.add(ByteRange.of(from, to));
                    progressUpdater.accept(to);
                }, deterministicTaskQueue.getThreadPool().generic())
            );
        }
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(futures.stream().allMatch(Future::isDone));
        return mergeContiguousRanges(ranges);
    }

    public static long numberOfRanges(long fileSize, long rangeSize) {
        return numberOfRanges(BlobCacheUtils.toIntBytes(fileSize), BlobCacheUtils.toIntBytes(rangeSize));
    }

    static long numberOfRanges(int fileSize, int rangeSize) {
        long numberOfRanges = fileSize / rangeSize;
        if (fileSize % rangeSize > 0) {
            numberOfRanges++;
        }
        if (numberOfRanges == 0) {
            numberOfRanges++;
        }
        return numberOfRanges;
    }

    /**
     * Generates a sorted set of non-empty and non-contiguous random ranges that could fit into a file of a given maximum length.
     */
    public static SortedSet<ByteRange> randomRanges(long length) {
        final SortedSet<ByteRange> randomRanges = new TreeSet<>();
        for (long i = 0L; i < length;) {
            long start = randomLongBetween(i, Math.max(0L, length - 1L));
            long end = randomLongBetween(start + 1L, length); // +1 for non empty ranges
            randomRanges.add(ByteRange.of(start, end));
            i = end + 1L + randomLongBetween(0L, Math.max(0L, length - end)); // +1 for non contiguous ranges
        }
        return randomRanges;
    }

    public static SortedSet<ByteRange> mergeContiguousRanges(final SortedSet<ByteRange> ranges) {
        return ranges.stream().collect(TreeSet::new, (gaps, gap) -> {
            if (gaps.isEmpty()) {
                gaps.add(gap);
            } else {
                final ByteRange previous = gaps.pollLast();
                if (previous.end() == gap.start()) {
                    gaps.add(ByteRange.of(previous.start(), gap.end()));
                } else {
                    gaps.add(previous);
                    gaps.add(gap);
                }
            }
        }, (gaps1, gaps2) -> {
            if (gaps1.isEmpty() == false && gaps2.isEmpty() == false) {
                final ByteRange last = gaps1.pollLast();
                final ByteRange first = gaps2.pollFirst();
                if (last.end() == first.start()) {
                    gaps1.add(ByteRange.of(last.start(), first.end()));
                } else {
                    gaps1.add(last);
                    gaps2.add(first);
                }
            }
            gaps1.addAll(gaps2);
        });
    }

    public static void assertCacheFileEquals(CacheFile expected, CacheFile actual) {
        MatcherAssert.assertThat(actual.getLength(), equalTo(expected.getLength()));
        MatcherAssert.assertThat(actual.getFile(), equalTo(expected.getFile()));
        MatcherAssert.assertThat(actual.getCacheKey(), equalTo(expected.getCacheKey()));
        MatcherAssert.assertThat(actual.getCompletedRanges(), equalTo(expected.getCompletedRanges()));
    }

    public static long sumOfCompletedRangesLengths(CacheFile cacheFile) {
        return cacheFile.getCompletedRanges().stream().mapToLong(ByteRange::length).sum();
    }

    /**
     * A {@link FileSystemProvider} that counts the number of times the method {@link FileChannel#force(boolean)} is executed on every
     * files.
     */
    public static class FSyncTrackingFileSystemProvider extends FilterFileSystemProvider {

        private final Map<Path, AtomicInteger> files = new ConcurrentHashMap<>();
        private final AtomicBoolean failFSyncs = new AtomicBoolean();
        private final FileSystem delegateInstance;
        private final Path rootDir;

        public FSyncTrackingFileSystemProvider(FileSystem delegate, Path rootDir) {
            super("fsynccounting://", delegate);
            this.rootDir = new FilterPath(rootDir, this.fileSystem);
            this.delegateInstance = delegate;
        }

        public void failFSyncs(boolean shouldFail) {
            failFSyncs.set(shouldFail);
        }

        public Path resolve(String other) {
            return rootDir.resolve(other);
        }

        @Nullable
        public Integer getNumberOfFSyncs(Path path) {
            final AtomicInteger counter = files.get(path);
            return counter != null ? counter.get() : null;
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            final AtomicInteger counter = files.computeIfAbsent(path, p -> new AtomicInteger(0));
            return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {

                @Override
                public void force(boolean metaData) throws IOException {
                    if (failFSyncs.get()) {
                        throw new IOException("simulated");
                    }
                    super.force(metaData);
                    counter.incrementAndGet();
                }
            };
        }

        public void tearDown() {
            PathUtilsForTesting.installMock(delegateInstance);
        }
    }
}
