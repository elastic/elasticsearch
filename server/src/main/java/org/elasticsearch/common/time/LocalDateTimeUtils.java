/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class LocalDateTimeUtils {
    public static LocalDateTime truncateToMonths(LocalDateTime dateTime, int months) {
        int totalMonths = (dateTime.getYear() - 1) * 12 + dateTime.getMonthValue() - 1;
        int truncatedMonths = Math.floorDiv(totalMonths, months) * months;
        return LocalDateTime.of(LocalDate.of(truncatedMonths / 12 + 1, truncatedMonths % 12 + 1, 1), LocalTime.MIDNIGHT);
    }

    public static LocalDateTime truncateToYears(LocalDateTime dateTime, int years) {
        int truncatedYear = Math.floorDiv(dateTime.getYear() - 1, years) * years + 1;
        return LocalDateTime.of(LocalDate.of(truncatedYear, 1, 1), LocalTime.MIDNIGHT);
    }
}
