/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyUploadedException;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.stateless.cache.reader.CacheFileReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class BlobCacheIndexInput extends BlobCacheBufferedIndexInput implements DirectAccessInput {

    /**
     * Same as org.apache.lucene.store.IOContext#DEFAULT, except does not warn on missing files.
     */
    public static final IOContext WARMING = IOContext.DEFAULT.withHints(DataAccessHint.SEQUENTIAL, WarmingHint.INSTANCE);

    private static final Logger logger = LogManager.getLogger(BlobCacheIndexInput.class);

    private final CacheFileReader cacheFileReader;
    private final AtomicBoolean closed;
    private final Releasable releasable;
    private final IOContext context;
    private final long offset;
    private final String sliceDescription;

    public BlobCacheIndexInput(
        String name,
        IOContext context,
        CacheFileReader cacheFileReader,
        Releasable releasable,
        long length,
        long offset,
        String sliceDescription
    ) {
        super(name, context, length);
        this.cacheFileReader = cacheFileReader;
        this.closed = new AtomicBoolean(false);
        this.releasable = releasable;
        this.context = context;
        this.offset = offset;
        this.sliceDescription = sliceDescription;
    }

    public BlobCacheIndexInput(
        String name,
        IOContext context,
        CacheFileReader cacheFileReader,
        Releasable releasable,
        long length,
        long offset
    ) {
        this(name, context, cacheFileReader, releasable, length, offset, null);
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        BlobCacheUtils.ensureSeek(pos, this);
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            Releasables.close(releasable);
        }
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
        BlobCacheUtils.ensureSlice(sliceDescription, offset, length, this);
        var arraySlice = trySliceBuffer(sliceDescription, offset, length);
        if (arraySlice != null) {
            return arraySlice;
        }
        return doSlice(sliceDescription, offset, length);
    }

    IndexInput doSlice(String sliceDescription, long offset, long length) {
        return new BlobCacheIndexInput(
            "(" + sliceDescription + ") " + super.toString(),
            context,
            cacheFileReader.copy(),
            null,
            length,
            this.offset + offset,
            sliceDescription
        );
    }

    @Override
    public IndexInput clone() {
        var bufferClone = tryCloneBuffer();
        if (bufferClone != null) {
            return bufferClone;
        }
        var clone = new BlobCacheIndexInput(
            super.toString(),
            context,
            cacheFileReader.copy(),
            null,
            length(),
            offset,
            sliceDescription != null ? sliceDescription : super.toString()
        );
        try {
            clone.seek(getFilePointer());
        } catch (IOException e) {
            assert false : e;
            throw new UncheckedIOException(e);
        }
        return clone;
    }

    private long getAbsolutePosition() {
        return getFilePointer() + offset;
    }

    String getSliceDescription() {
        return sliceDescription;
    }

    @Override
    public boolean withByteBufferSlice(long offset, long length, CheckedConsumer<ByteBuffer, IOException> action) throws IOException {
        return cacheFileReader.withByteBufferSlice(this.offset + offset, Math.toIntExact(length), action);
    }

    @Override
    public boolean withByteBufferSlices(long[] offsets, int length, int count, CheckedConsumer<ByteBuffer[], IOException> action)
        throws IOException {
        if (DirectAccessInput.checkSlicesArgs(offsets, count)) {
            return false;
        }
        long[] adjusted = offsets;
        if (this.offset != 0) {
            adjusted = new long[count];
            for (int i = 0; i < count; i++) {
                adjusted[i] = offsets[i] + this.offset;
            }
        }
        return cacheFileReader.withByteBufferSlices(adjusted, length, count, action);
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
        cacheFileReader.tryPrefetch(this.offset + offset, length);
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        try {
            doReadInternal(b);
        } catch (Exception e) {
            if (ExceptionsHelper.unwrap(e, FileNotFoundException.class, NoSuchFileException.class) != null) {
                // includes the file name of the BlobCacheIndexInput
                logger.log(context.hints().contains(WarmingHint.INSTANCE) ? Level.DEBUG : Level.WARN, () -> this + " did not find file", e);
            }
            if (e instanceof IOException ioe) {
                throw ioe;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new IOException(e);
            }
        }
    }

    private void doReadInternal(ByteBuffer b) throws Exception {
        final long position = getAbsolutePosition();
        final int length = b.remaining();
        try {
            logger.debug("reading file: {}, position: {}, length: {}", super.toString(), position, length);
            if (cacheFileReader.tryRead(b, position)) {
                return;
            }
        } catch (Exception e) {
            logger.debug("fast-path read failed to acquire cache page", e);
        }
        int positionBeforeRetry = b.position();
        int limitBeforeRetry = b.limit();
        // Normally we expect reads that overlap to synchronize through the SparseFileTracker so that only one read
        // fills the cache from the source, and the others subscribe to the gaps it fills. Under these circumstances
        // we would only need a single retry for ResourceAlreadyUploadedException, because only the read that has claimed
        // the gap would receive it from the index shard, and it would mark the commit as uploaded before completing the
        // read so that the next read would go to the object store.
        // However, if a read cannot claim a cache slot, then it does not synchronize with other readers over the read
        // range before contacting the index shard. In this case another read that does claim gaps in the cache may
        // also be issued to the index shard. If the read that couldn't claim a cache slot fails with RAUE, it will retry
        // and may then subscribe to the gap already claimed by the other in-flight read, which was issued to the object
        // store before the commit was marked uploaded and so can also fail with RAUE, triggering a second failure on the
        // subscribed read.
        // It should not be possible to see more than two retries, because any retry happens after the commit has been
        // marked uploaded, and either:
        // a) it fails to get a cache entry and will do its own read, see the commit has been uploaded, and go to the object store, or
        // b) it gets a cache entry and synchronizes on a gap. Gaps will only generate a single RAUE because the synchronization around
        // claiming the gap means that any second attempt to fill a gap must have seen that the commit was uploaded.
        for (int retries = 0; true; retries++) {
            try {
                readInternalSlow(b, position, length);
                break;
            } catch (Exception ex) {
                if (retries < 2 && ExceptionsHelper.unwrap(ex, ResourceAlreadyUploadedException.class) != null) {
                    logger.debug(() -> this + " already uploaded, retrying", ex);
                    assert b.position() == positionBeforeRetry : b.position() + " != " + positionBeforeRetry;
                    assert b.limit() == limitBeforeRetry : b.limit() + " != " + limitBeforeRetry;
                } else {
                    throw ex;
                }
            }
        }
    }

    private void readInternalSlow(ByteBuffer b, long position, int length) throws Exception {
        cacheFileReader.read(
            this,
            b,
            position,
            length,
            this.offset + length(),
            sliceDescription != null ? sliceDescription : super.toString()
        );
    }

    // package-private for testing
    CacheFileReader getCacheFileReader() {
        return cacheFileReader;
    }

    @Override
    public String toString() {
        return "BlobCacheIndexInput{["
            + super.toString()
            + "], context="
            + context
            + ", cacheFileReader="
            + cacheFileReader
            + ", length="
            + length()
            + ", offset="
            + offset
            + ", sliceDescription='="
            + sliceDescription
            + '}';
    }
}
