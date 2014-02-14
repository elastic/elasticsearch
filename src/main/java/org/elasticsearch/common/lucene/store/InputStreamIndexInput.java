/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
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
        if ((indexInput.length() - indexInput.getFilePointer()) > limit) {
            actualSizeToRead = limit;
        } else {
            actualSizeToRead = indexInput.length() - indexInput.getFilePointer();
        }
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
