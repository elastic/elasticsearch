/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

public class UTF8TopNEncoder implements TopNEncoder {

    private static final int CONTINUATION_BYTE = 0b1000_0000;

    @Override
    public void encodeBytesRef(BytesRef value, BytesRefBuilder bytesRefBuilder) {
        // add one bit to every byte so that there are no "0" bytes in the provided bytes. The only "0" bytes are
        // those defined as separators
        int end = value.offset + value.length;
        for (int i = value.offset; i < end; i++) {
            byte b = value.bytes[i];
            if ((b & CONTINUATION_BYTE) == 0) {
                b++;
            }
            bytesRefBuilder.append(b);
        }
    }

    @Override
    public String toString() {
        return "UTF8TopNEncoder";
    }
}
