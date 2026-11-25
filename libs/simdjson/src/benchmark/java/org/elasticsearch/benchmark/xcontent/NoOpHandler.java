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

import java.math.BigInteger;

/**
 * A "do nothing" handler, which records just enough to prevent dead-code elimination of the walk; every method is otherwise
 * as cheap as possible so the benchmark measures {@code walkObject}/{@code StringParser}, not
 * a handler implementation.
 */
final class NoOpHandler implements JsonDocumentHandler {
    long checksum;

    @Override
    public void startObject(String fieldName) {
    }

    @Override
    public void endObject() {
    }

    @Override
    public void emptyObject(String fieldName) {
    }

    @Override
    public void stringField(String fieldName, byte[] buf, int off, int len) {
        checksum ^= len;
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
    public void nullField(String fieldName) {
    }

    @Override
    public void startArray(String fieldName) {
    }

    @Override
    public void endArray() {
    }

    @Override
    public void arrayElemString(byte[] buf, int off, int len) {
        checksum ^= len;
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
    public void arrayElemNull() {
    }

    @Override
    public void arrayElemStartObject() {
    }

    @Override
    public void arrayElemEndObject() {
    }

    @Override
    public void arrayElemStartArray() {
    }

    @Override
    public void arrayElemEndArray() {
    }
}
