/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

enum BinaryDVCompressionMode {

    NO_COMPRESS((byte) 0),
    COMPRESSED_WITH_LZ4((byte) 1);

    final byte code;

    BinaryDVCompressionMode(byte code) {
        this.code = code;
    }

    static BinaryDVCompressionMode fromMode(byte mode) {
        return switch (mode) {
            case 0 -> NO_COMPRESS;
            case 1 -> COMPRESSED_WITH_LZ4;
            default -> throw new IllegalStateException("unknown compression mode [" + mode + "]");
        };
    }
}
