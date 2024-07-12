/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store.remote.filecache;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

import java.io.IOException;
import java.nio.file.Path;

public class FileCachedIndexInput extends IndexInput implements RandomAccessInput {

    protected final FileCache cache;

    /**
     * on disk file path of this index input
     */
    protected Path filePath;

    /**
     * underlying lucene index input which this IndexInput
     * delegate all its read functions to.
     */
    protected IndexInput luceneIndexInput;

    /** indicates if this IndexInput instance is a clone or not */
    private final boolean isClone;

    private volatile boolean closed = false;

    public FileCachedIndexInput(FileCache cache, Path filePath, IndexInput underlyingIndexInput) {
        this(cache, filePath, underlyingIndexInput, false);
    }

    FileCachedIndexInput(FileCache cache, Path filePath, IndexInput underlyingIndexInput, boolean isClone) {
        super("FileCachedIndexInput (path=" + filePath.toString() + ")");
        this.cache = cache;
        this.filePath = filePath;
        this.luceneIndexInput = underlyingIndexInput;
        this.isClone = isClone;
    }

    @Override
    public long getFilePointer() {
        return luceneIndexInput.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
        luceneIndexInput.seek(pos);
    }

    @Override
    public long length() {
        return luceneIndexInput.length();
    }

    @Override
    public byte readByte() throws IOException {
        return luceneIndexInput.readByte();
    }

    @Override
    public short readShort() throws IOException {
        return luceneIndexInput.readShort();
    }

    @Override
    public int readInt() throws IOException {
        return luceneIndexInput.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return luceneIndexInput.readLong();
    }

    @Override
    public final int readVInt() throws IOException {
        return luceneIndexInput.readVInt();
    }

    @Override
    public final long readVLong() throws IOException {
        return luceneIndexInput.readVLong();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        luceneIndexInput.readBytes(b, offset, len);
    }

    @Override
    public byte readByte(long pos) throws IOException {
        return ((RandomAccessInput) luceneIndexInput).readByte(pos);
    }

    @Override
    public short readShort(long pos) throws IOException {
        return ((RandomAccessInput) luceneIndexInput).readShort(pos);
    }

    @Override
    public int readInt(long pos) throws IOException {
        return ((RandomAccessInput) luceneIndexInput).readInt(pos);
    }

    @Override
    public long readLong(long pos) throws IOException {
        return ((RandomAccessInput) luceneIndexInput).readLong(pos);
    }

    @Override
    public FileCachedIndexInput clone() {
        cache.incRef(filePath);
        return new FileCachedIndexInput(cache, filePath, luceneIndexInput.clone(), true);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        // never reach here!
        throw new UnsupportedOperationException("FileCachedIndexInput couldn't be sliced.");
    }

    @SuppressWarnings("checkstyle:DescendantToken")
    @Override
    public void close() throws IOException {
        if (!closed) {
            // if the underlying lucene index input is a clone,
            // the following line won't close/unmap the file.
            luceneIndexInput.close();
            luceneIndexInput = null;
            // origin never reference it itself, only clone needs decRef here
            if (isClone) {
                cache.decRef(filePath);
            }
            closed = true;
        }
    }
}
