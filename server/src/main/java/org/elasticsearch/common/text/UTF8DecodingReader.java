/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.text;

import org.elasticsearch.xcontent.XContentString;

import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

/**
 * Reader that decodes UTF-8 formatted bytes into chars.
 */
public class UTF8DecodingReader extends Reader {
    private CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    private ByteBuffer bytes;

    public UTF8DecodingReader(ByteBuffer bytes) {
        this.bytes = bytes;
    }

    public UTF8DecodingReader(XContentString.UTF8Bytes utf8bytes) {
        this.bytes = ByteBuffer.wrap(utf8bytes.bytes(), utf8bytes.offset(), utf8bytes.length());
    }

    @Override
    public int read(char[] cbuf, int off, int len) {
        return read(CharBuffer.wrap(cbuf, off, len));
    }

    @Override
    public int read(CharBuffer cbuf) {
        if (bytes.hasRemaining() == false) {
            return -1;
        }

        int startPos = cbuf.position();
        decoder.decode(bytes, cbuf, true);
        return cbuf.position() - startPos;
    }

    @Override
    public void close() {}
}
