/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.io.InputStream;

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
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
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
}
