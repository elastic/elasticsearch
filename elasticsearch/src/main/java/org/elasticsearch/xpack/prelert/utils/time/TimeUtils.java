/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
     * time in milliseconds else if 10 or less digits it is in seconds. If that
     * fails it tries to parse the string using
     * {@link DateFieldMapper#DEFAULT_DATE_TIME_FORMATTER}
     *
     * If the date string cannot be parsed -1 is returned.
     *
     * @return The epoch time in milliseconds or -1 if the date cannot be
     *         parsed.
     */
    public static long dateStringToEpoch(String date) {
        try {
            long epoch = Long.parseLong(date);
            if (date.trim().length() <= 10) { // seconds
                return epoch * 1000;
            } else {
                return epoch;
            }
        } catch (NumberFormatException nfe) {
            // not a number
        }

        try {
            return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseMillis(date);
        } catch (IllegalArgumentException e) {
        }
        // Could not do the conversion
        return -1;
    }
}
