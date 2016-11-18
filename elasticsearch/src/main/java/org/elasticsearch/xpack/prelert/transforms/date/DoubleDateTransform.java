/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.transforms.date;

import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.Logger;

import org.elasticsearch.xpack.prelert.transforms.TransformException;

/**
 * A transformer that attempts to parse a String timestamp
 * as a double and convert that to a long that represents
 * an epoch time in seconds.
 * If isMillisecond is true, it assumes the number represents
 * time in milli-seconds and will convert to seconds
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

