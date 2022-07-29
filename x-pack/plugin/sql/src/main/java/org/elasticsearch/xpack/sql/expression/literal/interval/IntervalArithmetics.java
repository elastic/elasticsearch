/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.literal.interval;

import java.time.Duration;
import java.time.OffsetTime;
import java.time.Period;
import java.time.temporal.Temporal;

public final class IntervalArithmetics {

    private IntervalArithmetics() {}

    public static final long DAY_IN_MILLIS = 60 * 60 * 24 * 1000L;

    private enum IntervalOperation {
        ADD,
        SUB
    }

    public static Temporal add(Temporal l, Period r) {
        return periodArithmetics(l, r, IntervalOperation.ADD);
    }

    public static Temporal add(Temporal l, Duration r) {
        return durationArithmetics(l, r, IntervalOperation.ADD);
    }

    public static Temporal sub(Temporal l, Period r) {
        return periodArithmetics(l, r, IntervalOperation.SUB);
    }

    public static Temporal sub(Temporal l, Duration r) {
        return durationArithmetics(l, r, IntervalOperation.SUB);
    }

    private static Temporal periodArithmetics(Temporal l, Period r, IntervalOperation operation) {
        if (l == null || r == null) {
            return null;
        }

        if (l instanceof OffsetTime) {
            return l;
        }

        if (operation == IntervalOperation.ADD) {
            return l.plus(r);
        } else {
            return l.minus(r);
        }
    }

    private static Temporal durationArithmetics(Temporal l, Duration r, IntervalOperation operation) {
        if (l == null || r == null) {
            return null;
        }

        if (l instanceof OffsetTime) {
            r = Duration.ofMillis(r.toMillis() % DAY_IN_MILLIS);
        }

        if (operation == IntervalOperation.ADD) {
            return l.plus(r);
        } else {
            return l.minus(r);
        }
    }
}
