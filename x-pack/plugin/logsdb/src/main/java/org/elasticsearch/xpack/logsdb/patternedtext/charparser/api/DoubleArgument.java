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
 * Represents a double argument extracted from a text message.
 */
public final class DoubleArgument implements Argument<Double> {
    private final int startPosition;
    private final int length;
    private final double value;

    // for encoding
    private final byte[] doubleBytes = new byte[8];
    private final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();

    public DoubleArgument(String s, int startPosition, int length) {
        // todo - consider alternative for Double.parseDouble(String) that can work with CharSequence, the we can use SubstringView
        this(startPosition, length, Double.parseDouble(s.substring(startPosition, startPosition + length)));
    }

    public DoubleArgument(int startPosition, int length, double value) {
        this.startPosition = startPosition;
        this.length = length;
        this.value = value;
    }

    /**
     * NOTE: this method is boxing the double value into a Double object.
     * @return the value as a Double object
     */
    @Override
    public Double value() {
        return value;
    }

    @Override
    public EncodingType type() {
        return EncodingType.DOUBLE;
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
        ByteUtils.writeDoubleLE(value, doubleBytes, 0);
        return encoder.encodeToString(doubleBytes);
    }
}
