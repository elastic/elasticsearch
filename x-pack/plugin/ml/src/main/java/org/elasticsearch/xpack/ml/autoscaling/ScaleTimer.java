/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingDeciderService.DOWN_SCALE_DELAY;

/**
 * A timer for capturing the clock time when scaling decisions
 * are requested as well as for determining whether enough time
 * has passed for the {@link MlAutoscalingDeciderService#DOWN_SCALE_DELAY}.
 */
class ScaleTimer {

    private static final long NO_SCALE_DOWN_POSSIBLE = -1L;

    private final LongSupplier timeSupplier;
    private volatile long previousScaleTimeMs;
    private volatile long lastScaleTimeMs;
    private volatile long scaleDownDetected;

    ScaleTimer(LongSupplier timeSupplier) {
        this.timeSupplier = Objects.requireNonNull(timeSupplier);
        this.scaleDownDetected = NO_SCALE_DOWN_POSSIBLE;
    }

    void markScale() {
        previousScaleTimeMs = lastScaleTimeMs;
        lastScaleTimeMs = timeSupplier.getAsLong();
    }

    OptionalLong lastScaleToScaleIntervalMillis() {
        if (previousScaleTimeMs > 0L && lastScaleTimeMs > previousScaleTimeMs) {
            return OptionalLong.of(lastScaleTimeMs - previousScaleTimeMs);
        }
        return OptionalLong.empty();
    }

    long markDownScaleAndGetMillisLeftFromDelay(Settings configuration) {
        assert lastScaleTimeMs > 0L : "marked downscale without ever marking scale";
        final long now = timeSupplier.getAsLong();
        if (newScaleDownCheck()) {
            scaleDownDetected = now;
        }
        TimeValue downScaleDelay = DOWN_SCALE_DELAY.get(configuration);
        return downScaleDelay.millis() - (now - scaleDownDetected);
    }

    void resetScaleDownCoolDown() {
        this.scaleDownDetected = NO_SCALE_DOWN_POSSIBLE;
    }

    private boolean newScaleDownCheck() {
        return scaleDownDetected == NO_SCALE_DOWN_POSSIBLE;
    }

    long downScaleDetectedMillis() {
        return scaleDownDetected;
    }
}
