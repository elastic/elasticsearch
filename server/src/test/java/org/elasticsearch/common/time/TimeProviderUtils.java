/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import org.elasticsearch.core.TimeValue;

import java.util.function.LongSupplier;

public class TimeProviderUtils {

    /**
     * Creates a TimeProvider implementation for tests that uses the same source for
     * all methods (regardless of relative or absolute time).
     */
    public static TimeProvider create(LongSupplier timeSourceInMillis) {
        return new TimeProvider() {
            @Override
            public long relativeTimeInMillis() {
                return timeSourceInMillis.getAsLong();
            }

            @Override
            public long relativeTimeInNanos() {
                return timeSourceInMillis.getAsLong() * TimeValue.NSEC_PER_MSEC;
            }

            @Override
            public long rawRelativeTimeInMillis() {
                return timeSourceInMillis.getAsLong();
            }

            @Override
            public long absoluteTimeInMillis() {
                return timeSourceInMillis.getAsLong();
            }
        };
    }
}
