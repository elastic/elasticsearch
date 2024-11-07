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

public class TimeSupplierUtils {

    public static TimeSupplier create(LongSupplier timeSourceInMillis) {
        return new TimeSupplier() {
            @Override
            public long timeInMillis() {
                return timeSourceInMillis.getAsLong();
            }

            @Override
            public long timeInNanos() {
                return timeSourceInMillis.getAsLong() * TimeValue.NSEC_PER_MSEC;
            }
        };
    }
}
