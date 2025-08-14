/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned;

import java.util.Base64;

public abstract class ByteEncodedArgument implements Argument<byte[]> {
    protected final byte[] encodedBytes;
    protected final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();

    protected ByteEncodedArgument(int length) {
        this.encodedBytes = new byte[length];
    }

    @Override
    public byte[] value() {
        return encodedBytes;
    }

    @Override
    public String encode() {
        return encoder.encodeToString(encodedBytes);
    }
}
