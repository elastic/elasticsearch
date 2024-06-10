/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.time;

import org.elasticsearch.core.TimeValue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;

public interface RemainingTime extends Supplier<TimeValue> {
    static RemainingTime from(Supplier<Instant> currentTime, TimeValue remainingTime) {
        var timeout = currentTime.get().plus(remainingTime.duration(), remainingTime.timeUnit().toChronoUnit());
        var maxRemainingTime = remainingTime.nanos();
        return () -> {
            var remainingNanos = ChronoUnit.NANOS.between(currentTime.get(), timeout);
            return TimeValue.timeValueNanos(Math.max(0, Math.min(remainingNanos, maxRemainingTime)));
        };
    }
}
