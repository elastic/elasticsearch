
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

public enum BinaryDVCompressionMode {

    NO_COMPRESS((byte) 0),
    COMPRESSED_WITH_FSST((byte) 1);

    public final byte code;

    BinaryDVCompressionMode(byte code) {
        this.code = code;
    }

    public static BinaryDVCompressionMode fromMode(byte mode) {
        return switch (mode) {
            case 0 -> NO_COMPRESS;
            case 1 -> COMPRESSED_WITH_FSST;
            default -> throw new IllegalStateException("unknown compression mode [" + mode + "]");
        };
    }
}
