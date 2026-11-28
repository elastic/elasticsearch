/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.simdjson.JsonDocumentHandler;
import org.elasticsearch.simdjson.SimdJsonSupport;
import org.elasticsearch.simdjson.Utf8Value;

import java.math.BigInteger;

/**
 * The least work a handler can do with a document. The walk hands string values over unread, so each one is copied into
 * one reusable buffer, as an encoder copies it into a column. Every value is folded into a checksum, so that the walk is
 * not eliminated as dead code. A benchmark using this handler measures the walk and {@code StringParser}, not a handler.
 */
final class MinimalHandler implements JsonDocumentHandler {
    long checksum;
    private byte[] stringBuffer = new byte[256];

    @Override
    public void startObject(String fieldName) {}

    @Override
    public void endObject() {}

    @Override
    public void emptyObject(String fieldName) {}

    @Override
    public void stringField(String fieldName, byte[] buf, int off, int len) {
        checksum ^= len;
    }

    @Override
    public void rawStringField(String fieldName, Utf8Value value) {
        checksum ^= copy(value);
    }

    @Override
    public void longField(String fieldName, long value, boolean fitsInt, byte[] srcBuf, int srcOff, int srcLen) {
        checksum ^= value;
    }

    @Override
    public void bigIntegerField(String fieldName, BigInteger value, byte[] srcBuf, int srcOff, int srcLen) {
        checksum ^= value.longValue();
    }

    @Override
    public void doubleField(String fieldName, double value, boolean fitsFloat, byte[] srcBuf, int srcOff, int srcLen) {
        checksum ^= Double.doubleToRawLongBits(value);
    }

    @Override
    public void booleanField(String fieldName, boolean value, byte[] srcBuf, int srcOff, int srcLen) {
        checksum ^= value ? 1 : 0;
    }

    @Override
    public void nullField(String fieldName) {}

    @Override
    public void startArray(String fieldName) {}

    @Override
    public void endArray() {}

    @Override
    public void arrayElemString(byte[] buf, int off, int len) {
        checksum ^= len;
    }

    @Override
    public void rawArrayElemString(Utf8Value value) {
        checksum ^= copy(value);
    }

    private int copy(Utf8Value value) {
        int room = value.maxLength() + SimdJsonSupport.DESTINATION_PADDING;
        if (stringBuffer.length < room) {
            stringBuffer = new byte[Math.max(room, 2 * stringBuffer.length)];
        }
        return value.copyTo(stringBuffer, 0);
    }

    @Override
    public void arrayElemLong(long value, boolean fitsInt) {
        checksum ^= value;
    }

    @Override
    public void arrayElemBigInteger(BigInteger value, byte[] srcBuf, int srcOff, int srcLen) {
        checksum ^= value.longValue();
    }

    @Override
    public void arrayElemDouble(double value, boolean fitsFloat) {
        checksum ^= Double.doubleToRawLongBits(value);
    }

    @Override
    public void arrayElemBoolean(boolean value) {
        checksum ^= value ? 1 : 0;
    }

    @Override
    public void arrayElemNull() {}

    @Override
    public void arrayElemStartObject() {}

    @Override
    public void arrayElemEndObject() {}

    @Override
    public void arrayElemStartArray() {}

    @Override
    public void arrayElemEndArray() {}
}
