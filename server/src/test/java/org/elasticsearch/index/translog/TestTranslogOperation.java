/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

class TestTranslogOperation implements Translog.SizedWriteable {
    private final byte[] bytes;

    TestTranslogOperation(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public int getSizeInBytes() {
        return bytes.length;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.write(bytes);
    }
}
