/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;

/**
 * Represents an IPv4 address argument extracted from a text message.
 * <p>
 * The value is a byte array of the four octets of the IPv4 address.
 */
public final class IPv4Argument extends ByteEncodedArgument {

    public IPv4Argument(int[] octets) {
        super(4);
        for (int i = 0; i < 4; i++) {
            if (octets[i] < 0 || octets[i] > 255) {
                throw new IllegalArgumentException("Each octet of an IPv4 address must be between 0 and 255.");
            }
            encodedBytes[i] = (byte) octets[i];
        }
    }

    @Override
    public EncodingType type() {
        return EncodingType.IPV4;
    }
}
