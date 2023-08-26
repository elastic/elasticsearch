/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteArray;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * A @link {@link StreamInput} that consumes {@link ByteArray}.
 * This class is not Thread safe.
 */
public class BigBytesStreamInput extends InputStream {
    protected ByteArray bytes;
    protected int count;
    protected int current;

    public BigBytesStreamInput(ByteArray byteArray, int count){
        this.bytes = byteArray;
        this.count = count;
        this.current = 0;
    }

    @Override
    public void close() throws IOException {
        bytes.close();
        count = 0;
        current = 0;
    }

    @Override
    public int read() throws IOException {
        if(current >= count){
            return -1;
        }
        return bytes.get(current++);
    }

    @Override
    public int available() throws IOException {
        return count - current;
    }


    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int l = Math.min(len, available());
        BytesRef byteref = new BytesRef();
        bytes.get(current, l, byteref);
        System.arraycopy(byteref.bytes, byteref.offset, b, off, l);
        current += l;
        return l;
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        byte b[] = new byte[available()];
        read(b, 0, b.length);
        return b;
    }

}
