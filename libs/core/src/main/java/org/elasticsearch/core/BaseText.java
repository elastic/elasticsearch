/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import org.elasticsearch.core.bytes.BaseBytesArray;
import org.elasticsearch.core.bytes.BaseBytesReference;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Both {@link String} and {@link BaseBytesReference} representation of the text. Starts with one of those, and if
 * the other is requests, caches the other one in a local reference so no additional conversion will be needed.
 */
public class BaseText {
    protected BaseBytesReference bytes;
    private String text;
    private int length = -1;

    public BaseText(BaseBytesReference bytes) {
        this.bytes = bytes;
    }

    public BaseText(BaseBytesReference bytes, int length) {
        this.bytes = bytes;
        this.length = length;
    }

    public BaseText(String text) {
        this.text = text;
    }

    /**
     * Whether a {@link BaseBytesReference} view of the data is already materialized.
     */
    public boolean hasBytes() {
        return bytes != null;
    }

    /**
     * Returns a {@link BaseBytesReference} view of the data.
     */
    public BaseBytesReference bytes() {
        if (bytes == null) {
            bytes = new BaseBytesArray(text.getBytes(StandardCharsets.UTF_8));
        }
        return bytes;
    }

    /**
     * Whether a {@link String} view of the data is already materialized.
     */
    public boolean hasString() {
        return text != null;
    }

    /**
     * Returns a {@link String} view of the data.
     */
    public String string() {
        if (text == null) {
            text = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(bytes.array(), bytes.arrayOffset(), bytes.length())).toString();
        }
        return text;
    }

    @Override
    public String toString() {
        return string();
    }

    public int length() {
        if (length < 0) {
            length = string().length();
        }
        return length;
    }
}
