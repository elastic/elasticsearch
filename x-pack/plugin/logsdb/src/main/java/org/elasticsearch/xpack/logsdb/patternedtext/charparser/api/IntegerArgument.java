/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;

import java.util.Base64;

/**
 * Represents an integer argument extracted from a text message.
 */
public final class IntegerArgument implements Argument<Integer> {
    private final int startPosition;
    private final int length;
    private final int value;

    // for encoding
    private final byte[] integerBytes = new byte[4];
    private final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();

    public IntegerArgument(int startPosition, int length, int value) {
        this.startPosition = startPosition;
        this.length = length;
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
    public int startPosition() {
        return startPosition;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public String encode() {
        ByteUtils.writeIntLE(value, integerBytes, 0);
        return encoder.encodeToString(integerBytes);
    }
}
