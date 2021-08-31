/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import java.time.Instant;

// Timestamp implementation able to hold a timestamp with nanosecond accuracy.
public class NanosTimestamp extends Timestamp {
    // NB: doubles are not accurate enough to hold six digit micros with granularity for current dates.
    private final Instant timestamp;

    NanosTimestamp(long millis, long micros) {
        timestamp = Instant.ofEpochMilli(millis).plusNanos(micros);
    }

    @Override
    public Instant timestamp() {
        return timestamp;
    }

    public String asString() {
        long nanos = timestamp.getNano();
        long millisOfSecond = nanos / NANOS_PER_MILLI;
        return (timestamp.getEpochSecond() * MILLIS_PER_SECOND + millisOfSecond) + "." + (nanos - millisOfSecond * NANOS_PER_MILLI);
    }
}
