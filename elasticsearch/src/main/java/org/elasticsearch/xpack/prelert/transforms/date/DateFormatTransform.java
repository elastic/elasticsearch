/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

/**
 * A transform that attempts to parse a String timestamp
 * according to a timeFormat. It converts that
 * to a long that represents the equivalent epoch.
 */
public class DateFormatTransform extends DateTransform {
    private final String timeFormat;
    private final TimestampConverter dateToEpochConverter;

    public DateFormatTransform(String timeFormat, ZoneId defaultTimezone,
            List<TransformIndex> readIndexes, List<TransformIndex> writeIndexes, Logger logger) {
        super(readIndexes, writeIndexes, logger);

        this.timeFormat = timeFormat;
        dateToEpochConverter = DateTimeFormatterTimestampConverter.ofPattern(timeFormat, defaultTimezone);
    }

    @Override
    protected long toEpochMs(String field) throws TransformException {
        try {
            return dateToEpochConverter.toEpochMillis(field);
        } catch (DateTimeParseException pe) {
            String message = String.format(Locale.ROOT, "Cannot parse date '%s' with format string '%s'",
                    field, timeFormat);

            throw new ParseTimestampException(message);
        }
    }
}