/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobstore.cache.BlobStoreCacheService;
import org.elasticsearch.blobstore.cache.CachedBlob;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.index.store.IndexInputStats;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.toIntBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public final class TestUtils {
    private TestUtils() {}

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

    static SortedSet<Tuple<Long, Long>> mergeContiguousRanges(final SortedSet<Tuple<Long, Long>> ranges) {
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
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) {
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
            super(null, null, mock(Client.class), null);
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
}
