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
 * Represents a timestamp extracted from a text message.
 * <p>
 * The value is a long representing the number of milliseconds since the epoch.
 * It also holds the format of the timestamp as a string.
 */
public final class Timestamp implements Argument<Long> {

    private final long timestamp;
    private final String format;

    // for encoding
    private final byte[] millisBytes = new byte[8];
    private final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();

    public Timestamp(long timestamp, String format) {
        this.timestamp = timestamp;
        this.format = format;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getFormat() {
        return format;
    }

    /**
     * NOTE: this method is boxing the long value into a Long object.
     * @return the timestamp as a Long object
     */
    @Override
    public Long value() {
        return timestamp;
    }

    @Override
    public EncodingType type() {
        return EncodingType.TIMESTAMP;
    }

    @Override
    public String encode() {
        ByteUtils.writeLongLE(timestamp, millisBytes, 0);
        return encoder.encodeToString(millisBytes);
    }

    @Override
    public String toString() {
        return "Timestamp{" + "timestamp=" + timestamp + ", format='" + format + '\'' + '}';
    }
}
