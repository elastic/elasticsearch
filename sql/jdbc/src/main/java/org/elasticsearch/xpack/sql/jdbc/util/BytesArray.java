/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.util;

import org.elasticsearch.xpack.sql.net.client.util.Bytes;
import org.elasticsearch.xpack.sql.net.client.util.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class BytesArray {

    public static final byte[] EMPTY = new byte[0];

    byte[] bytes = EMPTY;
    int offset = 0;
    int size = 0;

    public BytesArray(int size) {
        this(new byte[size], 0, 0);
    }

    public BytesArray(byte[] data) {
        this(data, 0, data.length);
    }

    public BytesArray(byte[] data, int size) {
        this(data, 0, size);
    }

    public BytesArray(byte[] data, int offset, int size) {
        this.bytes = data;
        this.offset = offset;
        this.size = size;
    }

    public BytesArray(String source) {
        bytes(source);
    }

    public BytesArray(Bytes bytes) {
        this(bytes.bytes(), 0, bytes.size());
    }

    public byte[] bytes() {
        return bytes;
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return size;
    }

    public int capacity() {
        return bytes.length;
    }

    public int available() {
        return bytes.length - size;
    }

    public void bytes(byte[] array) {
        this.bytes = array;
        this.size = array.length;
        this.offset = 0;
    }

    public void bytes(byte[] array, int size) {
        this.bytes = array;
        this.size = size;
        this.offset = 0;
    }

    public void bytes(BytesArray ba) {
        this.bytes = ba.bytes;
        this.size = ba.size;
        this.offset = ba.offset;
    }

    public void bytes(String from) {
        size = 0;
        offset = 0;
        UnicodeUtil.UTF16toUTF8(from, 0, from.length(), this.bytes, 0);
    }

    public void size(int size) {
        this.size = size;
    }

    public void offset(int offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        // NOCOMMIT I think we're much more likely to want this as hex....
        return StringUtils.asUTFString(bytes, offset, size);
    }

    public void reset() {
        offset = 0;
        size = 0;
    }

    public void copyTo(BytesArray to) {
        to.add(bytes, offset, size);
    }

    public void add(int b) {
        int newcount = size + 1;
        checkSize(newcount);
        bytes[size] = (byte) b;
        size = newcount;
    }

    public void add(byte[] b) {
        if (b == null || b.length == 0) {
            return;
        }
        add(b, 0, b.length);
    }

    public void add(byte[] b, int off, int len) {
        if (len == 0) {
            return;
        }
        int newcount = size + len;
        checkSize(newcount);
        System.arraycopy(b, off, bytes, size, len);
        size = newcount;
    }

    public void add(String string) {
        if (string == null) {
            return;
        }
        add(string.getBytes(StandardCharsets.UTF_8));
    }

    private void checkSize(int newcount) {
        if (newcount > bytes.length) {
            bytes = ArrayUtils.grow(bytes, newcount);
        }
    }

    public void writeTo(OutputStream out) throws IOException {
        out.write(bytes, offset, size);
        out.flush();
    }
}