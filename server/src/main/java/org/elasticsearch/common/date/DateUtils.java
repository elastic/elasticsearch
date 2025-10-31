/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.date;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Utils class containing methods that convert between different java date formats
 */
public class DateUtils {

    /**
     * Converts milliseconds to a human-readable date time format
     * @param millis The epoch in milliseconds
     * @return A human-readable string containing the date time
     */
    public static String convertMillisToDateTime(long millis) {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.of("UTC"));
        return formatter.format(Instant.ofEpochMilli(millis));
    }
}
