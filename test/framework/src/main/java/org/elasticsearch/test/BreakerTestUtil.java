/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

public class BreakerTestUtil {
    private static final Logger logger = LogManager.getLogger(BreakerTestUtil.class);

    public static <E extends Exception> ByteSizeValue findBreakerLimit(ByteSizeValue tooBigToBreak, CheckedConsumer<ByteSizeValue, E> c)
        throws E {

        try {
            c.accept(tooBigToBreak);
        } catch (CircuitBreakingException e) {
            throw new IllegalArgumentException("expected runnable *not* to break under tooBigToBreak", e);
        }
        try {
            c.accept(ByteSizeValue.ofBytes(0));
            throw new IllegalArgumentException("expected runnable to break under a limit of 0 bytes");
        } catch (CircuitBreakingException e) {
            // desired
        }
        return ByteSizeValue.ofBytes(findBreakerLimit(0, tooBigToBreak.getBytes(), c));
    }

    private static <E extends Exception> long findBreakerLimit(long min, long max, CheckedConsumer<ByteSizeValue, E> c) throws E {
        while (min != max) {
            long diff = max - min;
            logger.info(
                "Between {} and {}. {} bytes remaining.",
                ByteSizeValue.ofBytes(min),
                ByteSizeValue.ofBytes(max),
                ByteSizeValue.ofBytes(diff)
            );
            diff /= 2;
            long mid = min + diff;
            try {
                c.accept(ByteSizeValue.ofBytes(mid));
                max = mid - 1;
            } catch (CircuitBreakingException e) {
                min = mid + 1;
            }
        }
        return min;
    }
}
