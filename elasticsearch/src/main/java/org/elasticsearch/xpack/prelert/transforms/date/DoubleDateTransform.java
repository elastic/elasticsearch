/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
public class DoubleDateTransform extends DateTransform {
    private final boolean isMillisecond;

    public DoubleDateTransform(boolean isMillisecond, List<TransformIndex> readIndexes,
            List<TransformIndex> writeIndexes, Logger logger) {
        super(readIndexes, writeIndexes, logger);
        this.isMillisecond = isMillisecond;
    }

    @Override
    protected long toEpochMs(String field) throws TransformException {
        try {
            long longValue = Double.valueOf(field).longValue();
            return isMillisecond ? longValue : longValue * SECONDS_TO_MS;
        } catch (NumberFormatException e) {
            String message = String.format(Locale.ROOT, "Cannot parse timestamp '%s' as epoch value", field);
            throw new ParseTimestampException(message);
        }
    }
}

