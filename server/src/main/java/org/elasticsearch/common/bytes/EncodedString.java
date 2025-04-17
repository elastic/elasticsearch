/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;

import java.util.Objects;

/**
 * Class that holds either a UTF-16 String or a UTF-8 BytesRef, and lazily converts between the two.
 */
public class EncodedString {
    private BytesRef bytesValue;
    private String stringValue;
    private final int charCount;

    public EncodedString(BytesRef bytesValue, int charCount) {
        this.bytesValue = Objects.requireNonNull(bytesValue);
        this.charCount = charCount;
    }

    public EncodedString(String stringValue) {
        this.stringValue = Objects.requireNonNull(stringValue);
        this.charCount = stringValue.length();
    }

    public BytesRef bytesValue() {
        if (bytesValue != null) {
            return bytesValue;
        }

        bytesValue = new BytesRef(stringValue);
        return bytesValue;
    }

    public String stringValue() {
        if (stringValue != null) {
            return stringValue;
        }

        stringValue = bytesValue.utf8ToString();
        assert stringValue.length() == charCount;
        return stringValue;
    }

    public int length() {
        return charCount;
    }
}
