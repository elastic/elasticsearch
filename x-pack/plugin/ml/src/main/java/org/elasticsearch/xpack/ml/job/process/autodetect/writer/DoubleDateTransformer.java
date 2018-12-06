/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import java.util.Locale;

/**
 * A transformer that attempts to parse a String timestamp
 * as a double and convert that to a long that represents
 * an epoch. If m_IsMillisecond is true, it will convert to seconds.
 */
public class DoubleDateTransformer implements DateTransformer {

    private static final long MS_IN_SECOND = 1000;

    private final boolean isMillisecond;

    public DoubleDateTransformer(boolean isMillisecond) {
        this.isMillisecond = isMillisecond;
    }

    @Override
    public long transform(String timestamp) throws CannotParseTimestampException {
        try {
            long longValue = Double.valueOf(timestamp).longValue();
            return isMillisecond ? longValue : longValue * MS_IN_SECOND;
        } catch (NumberFormatException e) {
            String message = String.format(Locale.ROOT, "Cannot parse timestamp '%s' as epoch value", timestamp);
            throw new CannotParseTimestampException(message, e);
        }
    }
}
