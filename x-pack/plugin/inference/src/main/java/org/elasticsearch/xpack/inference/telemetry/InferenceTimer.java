/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public record InferenceTimer(Instant startTime, Clock clock) {

    public InferenceTimer {
        Objects.requireNonNull(startTime);
        Objects.requireNonNull(clock);
    }

    public static InferenceTimer start() {
        return start(Clock.systemUTC());
    }

    public static InferenceTimer start(Clock clock) {
        return new InferenceTimer(clock.instant(), clock);
    }

    public long elapsedMillis() {
        return Duration.between(startTime(), clock().instant()).toMillis();
    }
}
