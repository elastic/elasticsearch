/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import java.time.Instant;

// Timestamp implementation able to hold a timestamp with millisecond accuracy.
public class MillisTimestamp extends Timestamp {
    private final long timestamp;

    public MillisTimestamp(long millis) {
        timestamp = millis;
    }

    @Override
    public int compareTo(Timestamp other) {
        return other instanceof MillisTimestamp ? Long.compare(timestamp, ((MillisTimestamp) other).millisTimestamp()) :
            super.compareTo(other);
    }

    @Override
    public long diff(Timestamp other) {
        return other instanceof MillisTimestamp ? (timestamp - ((MillisTimestamp) other).millisTimestamp()) * NANOS_PER_MILLI :
            diff(other, this);
    }

    @Override
    public Instant timestamp() {
        return Instant.ofEpochMilli(timestamp);
    }

    public String asString() {
        return String.valueOf(timestamp);
    }

    private long millisTimestamp() {
        return timestamp;
    }
}
