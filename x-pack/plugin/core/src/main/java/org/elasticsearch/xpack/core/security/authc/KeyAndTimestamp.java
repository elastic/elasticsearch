/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;

import java.io.IOException;
import java.util.Arrays;

public final class KeyAndTimestamp implements Writeable {
    private final SecureString key;
    private final long timestamp;

    public KeyAndTimestamp(SecureString key, long timestamp) {
        this.key = key;
        this.timestamp = timestamp;
    }

    KeyAndTimestamp(StreamInput input) throws IOException {
        timestamp = input.readVLong();
        byte[] keyBytes = input.readByteArray();
        final char[] ref = new char[keyBytes.length];
        int len = UnicodeUtil.UTF8toUTF16(keyBytes, 0, keyBytes.length, ref);
        key = new SecureString(Arrays.copyOfRange(ref, 0, len));
    }

    public long getTimestamp() {
        return timestamp;
    }

    public SecureString getKey() {
        return key;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        BytesRef bytesRef = new BytesRef(key);
        out.writeVInt(bytesRef.length);
        out.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyAndTimestamp that = (KeyAndTimestamp) o;

        if (timestamp != that.timestamp) return false;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }
}
