/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import java.io.EOFException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/** A Parquet file in memory that counts the requests, so that tests can check how many a read costs. */
public final class BytesParquetObject implements ParquetObject {

    private final String location;
    private final byte[] bytes;
    public final AtomicInteger requests = new AtomicInteger();

    public BytesParquetObject(String location, byte[] bytes) {
        this.location = location;
        this.bytes = bytes;
    }

    @Override
    public String location() {
        return location;
    }

    @Override
    public long length() {
        return bytes.length;
    }

    @Override
    public byte[] readRange(long position, int length) throws EOFException {
        if (position < 0 || position + length > bytes.length) {
            throw new EOFException("range [" + position + ", +" + length + ") is outside of " + bytes.length);
        }
        requests.incrementAndGet();
        return Arrays.copyOfRange(bytes, (int) position, (int) position + length);
    }
}
