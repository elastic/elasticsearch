/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobstore.cache.CopyOnReadInputStream;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public abstract class BaseSearchableSnapshotIndexInput extends BufferedIndexInput {

    public static final int DEFAULT_BUFFER_SIZE = Math.toIntExact(ByteSizeUnit.KB.toBytes(4L));

    protected final BlobContainer blobContainer;
    protected final FileInfo fileInfo;
    protected final IOContext context;
    protected final IndexInputStats stats;
    protected final long offset;
    protected final long length;

    // the following are only mutable so they can be adjusted after cloning/slicing
    protected volatile boolean isClone;
    private AtomicBoolean closed;

    public BaseSearchableSnapshotIndexInput(
        String resourceDesc,
        BlobContainer blobContainer,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        long offset,
        long length
    ) {
        super(resourceDesc, DEFAULT_BUFFER_SIZE); // TODO align buffer size with block size on disk and length of content cached for blobs
        this.blobContainer = Objects.requireNonNull(blobContainer);
        this.fileInfo = Objects.requireNonNull(fileInfo);
        this.context = Objects.requireNonNull(context);
        assert fileInfo.metadata()
            .hashEqualsContents() == false : "this method should only be used with blobs that are NOT stored in metadata's hash field "
                + "(fileInfo: "
                + fileInfo
                + ')';
        this.stats = Objects.requireNonNull(stats);
        this.offset = offset;
        this.length = length;
        this.closed = new AtomicBoolean(false);
        this.isClone = false;
    }

    public BaseSearchableSnapshotIndexInput(
        String resourceDesc,
        BlobContainer blobContainer,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        long offset,
        long length,
        int bufferSize
    ) {
        this(resourceDesc, blobContainer, fileInfo, context, stats, offset, length);
        setBufferSize(bufferSize);
    }

    @Override
    public final long length() {
        return length;
    }

    @Override
    public BaseSearchableSnapshotIndexInput clone() {
        final BaseSearchableSnapshotIndexInput clone = (BaseSearchableSnapshotIndexInput) super.clone();
        clone.closed = new AtomicBoolean(false);
        clone.isClone = true;
        return clone;
    }

    protected void ensureOpen() throws IOException {
        if (closed.get()) {
            throw new IOException(toString() + " is closed");
        }
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (isClone == false) {
                stats.incrementCloseCount();
            }
            innerClose();
        }
    }

    public abstract void innerClose() throws IOException;

    protected InputStream openInputStream(final long position, final long length) throws IOException {
        assert assertCurrentThreadMayAccessBlobStore();
        if (fileInfo.numberOfParts() == 1L) {
            assert position + length <= fileInfo.partBytes(0) : "cannot read ["
                + position
                + "-"
                + (position + length)
                + "] from ["
                + fileInfo
                + "]";
            return blobContainer.readBlob(fileInfo.partName(0L), position, length);
        } else {
            final long startPart = getPartNumberForPosition(position);
            final long endPart = getPartNumberForPosition(position + length);
            return new SlicedInputStream(endPart - startPart + 1L) {
                @Override
                protected InputStream openSlice(long slice) throws IOException {
                    final long currentPart = startPart + slice;
                    final long startInPart = (currentPart == startPart) ? getRelativePositionInPart(position) : 0L;
                    final long endInPart = (currentPart == endPart)
                        ? getRelativePositionInPart(position + length)
                        : getLengthOfPart(currentPart);
                    return blobContainer.readBlob(fileInfo.partName(currentPart), startInPart, endInPart - startInPart);
                }
            };
        }
    }

    protected InputStream openInputStream(
        final long position,
        final long length,
        final List<Tuple<Long, Integer>> regions,
        final TriConsumer<String, Long, ReleasableBytesReference> blobStoreCacher,
        final Logger logger
    ) throws IOException {
        final InputStream stream = openInputStream(position, length);
        if (regions == null || regions.isEmpty()) {
            return stream;
        }

        //
        // TODO I'm so sorry. This should be done differently, maybe using a smarter CopyOnReadInputStream
        //
        // The idea is to build a SequenceInputStream that wraps the stream from the blob store repository
        // into multiple limited streams that copy over the bytes of the regions.
        //
        // If while reading the stream we saw interesting regions to cache, we index them. It means that
        // we first have to sort the regions and exclude the ones that we're not going to see anyway.
        //
        // TODO we should check overlapping regions too

        final Iterator<Tuple<Long, Integer>> sortedRegions = regions.stream()
            .filter(region -> position <= region.v1())
            .filter(region -> region.v1() + region.v2() <= position + length)
            .sorted(Comparator.comparing(Tuple::v1))
            .collect(Collectors.toList())
            .iterator();

        final List<InputStream> streams = new ArrayList<>();
        for (long p = position; p < position + length;) {
            if (sortedRegions.hasNext()) {
                final Tuple<Long, Integer> nextRegion = sortedRegions.next();
                if (p < nextRegion.v1()) {
                    long limit = nextRegion.v1() - p;
                    streams.add(Streams.limitStream(Streams.noCloseStream(stream), limit));
                    p += limit;
                }
                assert p == nextRegion.v1();

                long limit = nextRegion.v2();
                streams.add(
                    new CopyOnReadInputStream(
                        Streams.limitStream(p + limit < length ? Streams.noCloseStream(stream) : stream, limit),
                        BigArrays.NON_RECYCLING_INSTANCE.newByteArray(limit),
                        ActionListener.wrap(releasable -> {
                            logger.trace(
                                () -> new ParameterizedMessage(
                                    "indexing bytes of file [{}] for region [{}-{}] in blob cache index",
                                    fileInfo.physicalName(),
                                    nextRegion.v1(),
                                    nextRegion.v1() + nextRegion.v2()
                                )
                            );
                            blobStoreCacher.apply(fileInfo.physicalName(), nextRegion.v1(), releasable);
                        }, e -> {
                            logger.trace(
                                () -> new ParameterizedMessage(
                                    "fail to index bytes of file [{}] for region [{}-{}] in blob cache index",
                                    fileInfo.physicalName(),
                                    nextRegion.v1(),
                                    nextRegion.v1() + nextRegion.v2()
                                ),
                                e
                            );
                        })
                    )
                );
                p += limit;
                assert p == nextRegion.v1() + nextRegion.v2();
            } else if (p < position + length) {
                long limit = position + length - p;
                streams.add(Streams.limitStream(stream, limit));
                p += limit;
            }
        }
        if (streams.size() == 1) {
            return streams.get(0);
        }
        return new SequenceInputStream(Collections.enumeration(streams));
    }

    protected final boolean assertCurrentThreadMayAccessBlobStore() {
        final String threadName = Thread.currentThread().getName();
        assert threadName.contains('[' + ThreadPool.Names.SNAPSHOT + ']')
            || threadName.contains('[' + ThreadPool.Names.GENERIC + ']')
            || threadName.contains('[' + ThreadPool.Names.SEARCH + ']')
            || threadName.contains('[' + ThreadPool.Names.SEARCH_THROTTLED + ']')

            // Cache asynchronous fetching runs on a dedicated thread pool.
            || threadName.contains('[' + SearchableSnapshotsConstants.CACHE_FETCH_ASYNC_THREAD_POOL_NAME + ']')

            // Cache prewarming also runs on a dedicated thread pool.
            || threadName.contains('[' + SearchableSnapshotsConstants.CACHE_PREWARMING_THREAD_POOL_NAME + ']')

            // Unit tests access the blob store on the main test thread; simplest just to permit this rather than have them override this
            // method somehow.
            || threadName.startsWith("TEST-")
            || threadName.startsWith("LuceneTestCase") : "current thread [" + Thread.currentThread() + "] may not read " + fileInfo;
        return true;
    }

    private long getPartNumberForPosition(long position) {
        ensureValidPosition(position);
        final long part = position / fileInfo.partSize().getBytes();
        assert part <= fileInfo.numberOfParts() : "part number [" + part + "] exceeds number of parts: " + fileInfo.numberOfParts();
        assert part >= 0L : "part number [" + part + "] is negative";
        return part;
    }

    private long getRelativePositionInPart(long position) {
        ensureValidPosition(position);
        final long pos = position % fileInfo.partSize().getBytes();
        assert pos < fileInfo.partBytes((int) getPartNumberForPosition(pos)) : "position in part [" + pos + "] exceeds part's length";
        assert pos >= 0L : "position in part [" + pos + "] is negative";
        return pos;
    }

    private long getLengthOfPart(long part) {
        return fileInfo.partBytes(Math.toIntExact(part));
    }

    private void ensureValidPosition(long position) {
        if (position < 0L || position > fileInfo.length()) {
            throw new IllegalArgumentException("Position [" + position + "] is invalid");
        }
    }
}
