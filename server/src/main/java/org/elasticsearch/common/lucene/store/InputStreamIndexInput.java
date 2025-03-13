/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class InputStreamIndexInput extends InputStream {

    private final IndexInput indexInput;

    private final long limit;

    private final long actualSizeToRead;

    private long counter = 0;

    private long markPointer;
    private long markCounter;

    public InputStreamIndexInput(IndexInput indexInput, long limit) {
        this.indexInput = indexInput;
        this.limit = limit;
        actualSizeToRead = Math.min((indexInput.length() - indexInput.getFilePointer()), limit);
    }

    public long actualSizeToRead() {
        return actualSizeToRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }
        if (indexInput.getFilePointer() >= indexInput.length()) {
            return -1;
        }
        if (indexInput.getFilePointer() + len > indexInput.length()) {
            len = (int) (indexInput.length() - indexInput.getFilePointer());
        }
        if (counter + len > limit) {
            len = (int) (limit - counter);
        }
        if (len <= 0) {
            return -1;
        }
        indexInput.readBytes(b, off, len, false);
        counter += len;
        return len;
    }

    @Override
    public int read() throws IOException {
        if (counter++ >= limit) {
            return -1;
        }
        return (indexInput.getFilePointer() < indexInput.length()) ? (indexInput.readByte() & 0xff) : -1;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        markPointer = indexInput.getFilePointer();
        markCounter = counter;
    }

    @Override
    public synchronized void reset() throws IOException {
        indexInput.seek(markPointer);
        counter = markCounter;
    }

    @Override
    public long skip(long n) throws IOException {
        long skipBytes = Math.min(n, Math.min(indexInput.length() - indexInput.getFilePointer(), limit - counter));
        if (skipBytes <= 0) {
            return 0;
        }
        indexInput.skipBytes(skipBytes);
        counter += skipBytes;
        return skipBytes;
    }
}
