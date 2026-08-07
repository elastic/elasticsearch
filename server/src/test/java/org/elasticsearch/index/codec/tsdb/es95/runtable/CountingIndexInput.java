/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * A thin {@link IndexInput} wrapper that tallies every byte read into a shared counter, propagating
 * the counter to clones and slices (including the random-access slices the {@link
 * org.apache.lucene.util.packed.DirectReader} path reads ordinals through) so a single count
 * captures all reads no matter which view served them.
 */
final class CountingIndexInput extends IndexInput {

    private final IndexInput in;
    private final long[] counter;

    CountingIndexInput(final IndexInput in, final long[] counter) {
        super("CountingIndexInput(" + in + ")");
        this.in = in;
        this.counter = counter;
    }

    @Override
    public byte readByte() throws IOException {
        counter[0]++;
        return in.readByte();
    }

    @Override
    public void readBytes(final byte[] b, final int offset, final int len) throws IOException {
        counter[0] += len;
        in.readBytes(b, offset, len);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public long getFilePointer() {
        return in.getFilePointer();
    }

    @Override
    public void seek(final long pos) throws IOException {
        in.seek(pos);
    }

    @Override
    public long length() {
        return in.length();
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        return new CountingIndexInput(in.slice(sliceDescription, offset, length), counter);
    }

    @Override
    public IndexInput clone() {
        return new CountingIndexInput(in.clone(), counter);
    }
}
