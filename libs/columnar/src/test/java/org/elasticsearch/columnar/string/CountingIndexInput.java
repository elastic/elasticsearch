/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

final class CountingIndexInput extends FilterIndexInput {

    private final AtomicLong bytesRead;

    CountingIndexInput(IndexInput in) {
        this(in.toString(), in, new AtomicLong());
    }

    private CountingIndexInput(String description, IndexInput in, AtomicLong bytesRead) {
        super(description, in);
        this.bytesRead = bytesRead;
    }

    long bytesRead() {
        return bytesRead.get();
    }

    @Override
    public byte readByte() throws IOException {
        bytesRead.incrementAndGet();
        return super.readByte();
    }

    @Override
    public void readBytes(byte[] destination, int offset, int length) throws IOException {
        bytesRead.addAndGet(length);
        super.readBytes(destination, offset, length);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        return new CountingIndexInput(sliceDescription, super.slice(sliceDescription, offset, length), bytesRead);
    }

    @Override
    public IndexInput clone() {
        return new CountingIndexInput(toString(), in.clone(), bytesRead);
    }
}
