/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.xcontent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Both {@link String} and {@link ByteBuffer} representation of the text. Starts with one of those, and if
 * the other is requests, caches the other one in a local reference so no additional conversion will be needed.
 */
public final class Text implements XContentString, Comparable<Text>, ToXContentFragment {

    public static final Text[] EMPTY_ARRAY = new Text[0];

    public static Text[] convertFromStringArray(String[] strings) {
        if (strings.length == 0) {
            return EMPTY_ARRAY;
        }
        Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new Text(strings[i]);
        }
        return texts;
    }

    private ByteBuffer bytes;
    private String text;
    private int hash;
    private int stringLength = -1;

    /**
     * Construct a Text from a UTF-8 encoded ByteBuffer. Since no string length is specified, {@link #stringLength()}
     * will perform a string conversion to measure the string length.
     */
    public Text(ByteBuffer bytes) {
        this.bytes = bytes;
    }

    /**
     * Construct a Text from a UTF-8 encoded ByteBuffer and an explicit string length. Used to avoid string conversion
     * in {@link #stringLength()}.
     */
    public Text(ByteBuffer bytes, int stringLength) {
        this.bytes = bytes;
        this.stringLength = stringLength;
    }

    public Text(String text) {
        this.text = text;
    }

    /**
     * Whether a {@link ByteBuffer} view of the data is already materialized.
     */
    public boolean hasBytes() {
        return bytes != null;
    }

    @Override
    public ByteBuffer bytes() {
        if (bytes == null) {
            bytes = StandardCharsets.UTF_8.encode(text);
        }
        return bytes;
    }

    /**
     * Whether a {@link String} view of the data is already materialized.
     */
    public boolean hasString() {
        return text != null;
    }

    @Override
    public String string() {
        if (text == null) {
            text = StandardCharsets.UTF_8.decode(bytes).toString();
        }
        return text;
    }

    @Override
    public int stringLength() {
        if (stringLength < 0) {
            stringLength = string().length();
        }
        return stringLength;
    }

    @Override
    public String toString() {
        return string();
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            hash = bytes().hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return bytes().equals(((Text) obj).bytes());
    }

    @Override
    public int compareTo(Text text) {
        return bytes().compareTo(text.bytes());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (hasString()) {
            return builder.value(this.string());
        } else {
            // TODO: TextBytesOptimization we can use a buffer here to convert it? maybe add a
            // request to jackson to support InputStream as well?
            assert bytes.hasArray();
            return builder.utf8Value(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining());
        }
    }
}
