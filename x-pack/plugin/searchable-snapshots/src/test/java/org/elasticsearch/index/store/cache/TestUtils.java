/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.index.store.cache;

import org.apache.lucene.mockfile.FilterFileChannel;
import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.apache.lucene.mockfile.FilterPath;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobstore.cache.BlobStoreCacheService;
import org.elasticsearch.blobstore.cache.CachedBlob;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.index.store.IndexInputStats;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
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
import static org.apache.lucene.util.LuceneTestCase.random;
import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public final class TestUtils {
    private TestUtils() {}

    public static SortedSet<Tuple<Long, Long>> randomPopulateAndReads(final CacheFile cacheFile) {
        return randomPopulateAndReads(cacheFile, (fileChannel, aLong, aLong2) -> {});
    }

    public static SortedSet<Tuple<Long, Long>> randomPopulateAndReads(CacheFile cacheFile, TriConsumer<FileChannel, Long, Long> consumer) {
        final SortedSet<Tuple<Long, Long>> ranges = synchronizedNavigableSet(new TreeSet<>(Comparator.comparingLong(Tuple::v1)));
        final List<Future<Integer>> futures = new ArrayList<>();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            builder().put(NODE_NAME_SETTING.getKey(), "_node").build(),
            random()
        );
        for (int i = 0; i < between(0, 10); i++) {
            final long start = randomLongBetween(0L, Math.max(0L, cacheFile.getLength() - 1L));
            final long end = randomLongBetween(Math.min(start + 1L, cacheFile.getLength()), cacheFile.getLength());
            final Tuple<Long, Long> range = Tuple.tuple(start, end);
            futures.add(
                cacheFile.populateAndRead(range, range, channel -> Math.toIntExact(end - start), (channel, from, to, progressUpdater) -> {
                    consumer.apply(channel, from, to);
                    ranges.add(Tuple.tuple(from, to));
                    progressUpdater.accept(to);
                }, deterministicTaskQueue.getThreadPool().generic())
            );
        }
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(futures.stream().allMatch(Future::isDone));
        return mergeContiguousRanges(ranges);
    }

    public static long numberOfRanges(long fileSize, long rangeSize) {
        return numberOfRanges(toIntBytes(fileSize), toIntBytes(rangeSize));
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
    public static SortedSet<Tuple<Long, Long>> randomRanges(long length) {
        final SortedSet<Tuple<Long, Long>> randomRanges = new TreeSet<>(Comparator.comparingLong(Tuple::v1));
        for (long i = 0L; i < length;) {
            long start = randomLongBetween(i, Math.max(0L, length - 1L));
            long end = randomLongBetween(start + 1L, length); // +1 for non empty ranges
            randomRanges.add(Tuple.tuple(start, end));
            i = end + 1L + randomLongBetween(0L, Math.max(0L, length - end)); // +1 for non contiguous ranges
        }
        return randomRanges;
    }

    public static SortedSet<Tuple<Long, Long>> mergeContiguousRanges(final SortedSet<Tuple<Long, Long>> ranges) {
        // Eclipse needs the TreeSet type to be explicit (see https://bugs.eclipse.org/bugs/show_bug.cgi?id=568600)
        return ranges.stream().collect(() -> new TreeSet<Tuple<Long, Long>>(Comparator.comparingLong(Tuple::v1)), (gaps, gap) -> {
            if (gaps.isEmpty()) {
                gaps.add(gap);
            } else {
                final Tuple<Long, Long> previous = gaps.pollLast();
                if (previous.v2().equals(gap.v1())) {
                    gaps.add(Tuple.tuple(previous.v1(), gap.v2()));
                } else {
                    gaps.add(previous);
                    gaps.add(gap);
                }
            }
        }, (gaps1, gaps2) -> {
            if (gaps1.isEmpty() == false && gaps2.isEmpty() == false) {
                final Tuple<Long, Long> last = gaps1.pollLast();
                final Tuple<Long, Long> first = gaps2.pollFirst();
                if (last.v2().equals(first.v1())) {
                    gaps1.add(Tuple.tuple(last.v1(), first.v2()));
                } else {
                    gaps1.add(last);
                    gaps2.add(first);
                }
            }
            gaps1.addAll(gaps2);
        });
    }

    public static void assertCacheFileEquals(CacheFile expected, CacheFile actual) {
        assertThat(actual.getLength(), equalTo(expected.getLength()));
        assertThat(actual.getFile(), equalTo(expected.getFile()));
        assertThat(actual.getCacheKey(), equalTo(expected.getCacheKey()));
        assertThat(actual.getCompletedRanges(), equalTo(expected.getCompletedRanges()));
    }

    public static void assertCounter(IndexInputStats.Counter counter, long total, long count, long min, long max) {
        assertThat(counter.total(), equalTo(total));
        assertThat(counter.count(), equalTo(count));
        assertThat(counter.min(), equalTo(min));
        assertThat(counter.max(), equalTo(max));
    }

    public static void assertCounter(
        IndexInputStats.TimedCounter timedCounter,
        long total,
        long count,
        long min,
        long max,
        long totalNanoseconds
    ) {
        assertCounter(timedCounter, total, count, min, max);
        assertThat(timedCounter.totalNanoseconds(), equalTo(totalNanoseconds));
    }

    public static long sumOfCompletedRangesLengths(CacheFile cacheFile) {
        return cacheFile.getCompletedRanges().stream().mapToLong(range -> range.v2() - range.v1()).sum();
    }

    /**
     * A {@link BlobContainer} that can read a single in-memory blob.
     * Any attempt to read a different blob will throw a {@link FileNotFoundException}
     */
    public static BlobContainer singleBlobContainer(final String blobName, final byte[] blobContent) {
        return new MostlyUnimplementedFakeBlobContainer() {
            @Override
            public InputStream readBlob(String name, long position, long length) throws IOException {
                if (blobName.equals(name) == false) {
                    throw new FileNotFoundException("Blob not found: " + name);
                }
                return Streams.limitStream(
                    new ByteArrayInputStream(blobContent, toIntBytes(position), blobContent.length - toIntBytes(position)),
                    length
                );
            }
        };
    }

    static BlobContainer singleSplitBlobContainer(final String blobName, final byte[] blobContent, final int partSize) {
        if (partSize >= blobContent.length) {
            return singleBlobContainer(blobName, blobContent);
        } else {
            final String prefix = blobName + ".part";
            return new MostlyUnimplementedFakeBlobContainer() {
                @Override
                public InputStream readBlob(String name, long position, long length) throws IOException {
                    if (name.startsWith(prefix) == false) {
                        throw new FileNotFoundException("Blob not found: " + name);
                    }
                    assert position + length <= partSize : "cannot read ["
                        + position
                        + "-"
                        + (position + length)
                        + "] from array part of length ["
                        + partSize
                        + "]";
                    final int partNumber = Integer.parseInt(name.substring(prefix.length()));
                    final int positionInBlob = toIntBytes(position) + partSize * partNumber;
                    assert positionInBlob + length <= blobContent.length : "cannot read ["
                        + positionInBlob
                        + "-"
                        + (positionInBlob + length)
                        + "] from array of length ["
                        + blobContent.length
                        + "]";
                    return Streams.limitStream(
                        new ByteArrayInputStream(blobContent, positionInBlob, blobContent.length - positionInBlob),
                        length
                    );
                }
            };
        }
    }

    private static class MostlyUnimplementedFakeBlobContainer implements BlobContainer {

        @Override
        public long readBlobPreferredLength() {
            return Long.MAX_VALUE;
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() {
            throw unsupportedException();
        }

        @Override
        public BlobPath path() {
            throw unsupportedException();
        }

        @Override
        public boolean blobExists(String blobName) {
            throw unsupportedException();
        }

        @Override
        public InputStream readBlob(String blobName) {
            throw unsupportedException();
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            throw unsupportedException();
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) {
            throw unsupportedException();
        }

        @Override
        public void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) {
            throw unsupportedException();
        }

        @Override
        public DeleteResult delete() {
            throw unsupportedException();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) {
            throw unsupportedException();
        }

        @Override
        public Map<String, BlobContainer> children() {
            throw unsupportedException();
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) {
            throw unsupportedException();
        }

        private UnsupportedOperationException unsupportedException() {
            assert false : "this operation is not supported and should have not be called";
            return new UnsupportedOperationException("This operation is not supported");
        }
    }

    public static class NoopBlobStoreCacheService extends BlobStoreCacheService {

        public NoopBlobStoreCacheService() {
            super(null, mock(Client.class), null);
        }

        @Override
        protected void getAsync(String repository, String name, String path, long offset, ActionListener<CachedBlob> listener) {
            listener.onResponse(CachedBlob.CACHE_NOT_READY);
        }

        @Override
        public void putAsync(
            String repository,
            String name,
            String path,
            long offset,
            BytesReference content,
            ActionListener<Void> listener
        ) {
            listener.onResponse(null);
        }
    }

    public static class SimpleBlobStoreCacheService extends BlobStoreCacheService {

        private final ConcurrentHashMap<String, CachedBlob> blobs = new ConcurrentHashMap<>();

        public SimpleBlobStoreCacheService() {
            super(null, mock(Client.class), null);
        }

        @Override
        protected void getAsync(String repository, String name, String path, long offset, ActionListener<CachedBlob> listener) {
            CachedBlob blob = blobs.get(CachedBlob.generateId(repository, name, path, offset));
            if (blob != null) {
                listener.onResponse(blob);
            } else {
                listener.onResponse(CachedBlob.CACHE_MISS);
            }
        }

        @Override
        public void putAsync(
            String repository,
            String name,
            String path,
            long offset,
            BytesReference content,
            ActionListener<Void> listener
        ) {
            final CachedBlob cachedBlob = new CachedBlob(
                Instant.ofEpochMilli(0),
                Version.CURRENT,
                repository,
                name,
                path,
                new BytesArray(content.toBytesRef(), true),
                offset
            );
            blobs.put(cachedBlob.generatedId(), cachedBlob);
            listener.onResponse(null);
        }
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
