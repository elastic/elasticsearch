/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils.time;

import java.time.format.DateTimeParseException;

/**
 * A converter that enables conversions of textual timestamps to epoch seconds
 * or milliseconds according to a given pattern.
 */
public interface TimestampConverter {
    /**
     * Converts the a textual timestamp into an epoch in seconds
     *
     * @param timestamp the timestamp to convert, not null. The timestamp is expected to
     * be formatted according to the pattern of the formatter. In addition, the pattern is
     * assumed to contain both date and time information.
     * @return the epoch in seconds for the given timestamp
     * @throws DateTimeParseException if unable to parse the given timestamp
     */
    long toEpochSeconds(String timestamp);

    /**
     * Converts the a textual timestamp into an epoch in milliseconds
     *
     * @param timestamp the timestamp to convert, not null. The timestamp is expected to
     * be formatted according to the pattern of the formatter. In addition, the pattern is
     * assumed to contain both date and time information.
     * @return the epoch in milliseconds for the given timestamp
     * @throws DateTimeParseException if unable to parse the given timestamp
     */
    long toEpochMillis(String timestamp);
}
