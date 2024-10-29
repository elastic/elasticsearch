/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.XContentInputDecorator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ByteTrackingXContentInputDecorator implements XContentInputDecorator {
    private long readOffset = 0;

    @Override
    public InputStream decorate(byte[] src, int offset, int length) {
        var stream = new ByteArrayInputStream(src, offset, length);

        return new InputStream() {
            @Override
            public int read() throws IOException {
                readOffset += 1;
                return stream.read();
            }
        };
    }

    @Override
    public InputStream decorate(InputStream in) {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                readOffset += 1;
                return in.read();
            }
        };
    }

    public long getReadOffset() {
        return readOffset;
    }
}
