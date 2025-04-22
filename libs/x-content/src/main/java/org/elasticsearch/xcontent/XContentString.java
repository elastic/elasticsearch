/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class XContentString {
    private String stringValue;
    private ByteRef bytesValue;
    private final int charCount;

    public record ByteRef(byte[] bytes, int offset, int length) {}

    public XContentString(String string) {
        this.stringValue = Objects.requireNonNull(string);
        this.charCount = string.length();
    }

    public XContentString(ByteRef bytes, int charCount) {
        this.bytesValue = Objects.requireNonNull(bytes);
        this.charCount = charCount;
    }

    public ByteRef getBytes() {
        if (bytesValue == null) {
            var byteBuffer = StandardCharsets.UTF_8.encode(stringValue);
            bytesValue = new ByteRef(byteBuffer.array(), 0, byteBuffer.limit());
        }
        return bytesValue;
    }

    public String getString() {
        if (stringValue == null) {
            stringValue = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(bytesValue.bytes(), bytesValue.offset(), bytesValue.length()))
                .toString();
            assert stringValue.length() == charCount;
        }
        return stringValue;
    }

    public int length() {
        return charCount;
    }

}
