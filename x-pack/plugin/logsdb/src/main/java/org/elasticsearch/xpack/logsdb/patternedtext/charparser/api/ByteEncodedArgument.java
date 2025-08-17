/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

import java.util.Base64;

/**
 * An abstract class for arguments that are encoded as a byte array.
 * <p>
 * This class provides a base implementation for arguments that are represented as a byte array.
 * It handles the storage of the byte array and provides a Base64 encoder for the `encode()` method.
 */
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
