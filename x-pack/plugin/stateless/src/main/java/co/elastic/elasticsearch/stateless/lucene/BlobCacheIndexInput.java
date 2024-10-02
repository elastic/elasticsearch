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
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.lucene;

import co.elastic.elasticsearch.stateless.cache.reader.CacheFileReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyUploadedException;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class BlobCacheIndexInput extends BlobCacheBufferedIndexInput {

    private static final Logger logger = LogManager.getLogger(BlobCacheIndexInput.class);

    private final CacheFileReader cacheFileReader;
    private final AtomicBoolean closed;
    private final Releasable releasable;
    private final IOContext context;
    private final long offset;

    public BlobCacheIndexInput(
        String name,
        IOContext context,
        CacheFileReader cacheFileReader,
        Releasable releasable,
        long length,
        long offset
    ) {
        super(name, context, length);
        this.cacheFileReader = cacheFileReader;
        this.closed = new AtomicBoolean(false);
        this.releasable = releasable;
        this.context = context;
        this.offset = offset;
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
            this.offset + offset
        );
    }

    @Override
    public IndexInput clone() {
        var bufferClone = tryCloneBuffer();
        if (bufferClone != null) {
            return bufferClone;
        }
        var clone = new BlobCacheIndexInput(super.toString(), context, cacheFileReader.copy(), null, length(), offset);
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

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        try {
            doReadInternal(b);
        } catch (IOException | RuntimeException e) {
            if (ExceptionsHelper.unwrap(e, FileNotFoundException.class, NoSuchFileException.class) != null) {
                logger.warn(() -> this + " did not find file", e); // includes the file name of the BlobCacheIndexInput
            }
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
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
        try {
            readInternalSlow(b, position, length);
        } catch (Exception ex) {
            if (ExceptionsHelper.unwrap(ex, ResourceAlreadyUploadedException.class) != null) {
                logger.debug(() -> this + " retrying from object store", ex);
                assert b.position() == positionBeforeRetry : b.position() + " != " + positionBeforeRetry;
                assert b.limit() == limitBeforeRetry : b.limit() + " != " + limitBeforeRetry;
                readInternalSlow(b, position, length);
                return;
            }
            throw ex;
        }
    }

    private void readInternalSlow(ByteBuffer b, long position, int length) throws Exception {
        cacheFileReader.read(this, b, position, length, this.offset + length());
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
            + '}';
    }
}
