/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;

/**
 * Represents a hexadecimal argument extracted from a text message.
 * <p>
 * The value is a byte array decoded from a hexadecimal string.
 */
public final class HexadecimalArgument extends ByteEncodedArgument {

    public HexadecimalArgument(String s, int startPosition, int length) {
        super(startPosition, length, (length + 1) / 2);
        int endIndex = startPosition + length;
        for (int i = startPosition, j = 0; i < endIndex; i += 2, j++) {
            int high = Character.digit(s.charAt(i), 16);
            if (i + 1 < endIndex) {
                int low = Character.digit(s.charAt(i + 1), 16);
                encodedBytes[j] = (byte) ((high << 4) | low);
            } else {
                // this is the last nibble for an odd-length string.
                // it should be treated as the low nibble of the last byte.
                encodedBytes[j] = (byte) high;
            }
        }
    }

    @Override
    public EncodingType type() {
        return EncodingType.HEX;
    }
}
