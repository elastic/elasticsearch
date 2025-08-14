/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.ByteUtils;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;

import java.util.Base64;

public final class IntegerArgument implements Argument<Integer> {
    private final int value;

    // for encoding
    private final byte[] integerBytes = new byte[4];
    private final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();

    public IntegerArgument(int value) {
        this.value = value;
    }

    /**
     * NOTE: this method is boxing the int value into a Integer object.
     * @return the value as an Integer object
     */
    @Override
    public Integer value() {
        return value;
    }

    @Override
    public EncodingType type() {
        return EncodingType.INTEGER;
    }

    @Override
    public String encode() {
        ByteUtils.writeIntLE(value, integerBytes, 0);
        return encoder.encodeToString(integerBytes);
    }
}
