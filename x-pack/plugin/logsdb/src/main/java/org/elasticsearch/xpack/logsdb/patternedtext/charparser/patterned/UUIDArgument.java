/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.ByteUtils;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;

import java.util.UUID;

public final class UUIDArgument extends ByteEncodedArgument {

    public UUIDArgument(String s, int start, int end) {
        super(16);
        int length = end - start;
        if (length == 36) {
            // UUID in standard format (e.g., "123e4567-e89b-12d3-a456-426614174000")
            UUID uuid = UUID.fromString(s.substring(start, end));
            ByteUtils.writeLongLE(uuid.getMostSignificantBits(), encodedBytes, 0);
            ByteUtils.writeLongLE(uuid.getLeastSignificantBits(), encodedBytes, 8);
        } else if (length == 32) {
            // UUID in compact format (e.g., "123e4567e89b12d3a456426614174000")
            // todo - handle this case
        }
    }

    @Override
    public EncodingType type() {
        return EncodingType.UUID;
    }
}
