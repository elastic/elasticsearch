/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;

import java.util.UUID;

public abstract class ExpirationCallback {

    static final String EXPIRATION_JOB_PREFIX = ".license_expiration_job_";

    public enum Orientation {PRE, POST}

    public abstract static class Pre extends ExpirationCallback {

        /**
         * Callback schedule prior to license expiry
         *
         * @param min       latest relative time to execute before license expiry
         * @param max       earliest relative time to execute before license expiry
         * @param frequency interval between execution
         */
        public Pre(TimeValue min, TimeValue max, TimeValue frequency) {
            super(Orientation.PRE, min, max, frequency);
        }
    }

    public abstract static class Post extends ExpirationCallback {

        /**
         * Callback schedule after license expiry
         *
         * @param min       earliest relative time to execute after license expiry
         * @param max       latest relative time to execute after license expiry
         * @param frequency interval between execution
         */
        public Post(TimeValue min, TimeValue max, TimeValue frequency) {
            super(Orientation.POST, min, max, frequency);
        }
    }

    private final String id;
    private final Orientation orientation;
    private final long min;
    private final long max;
    private final long frequency;

    private ExpirationCallback(Orientation orientation, TimeValue min, TimeValue max, TimeValue frequency) {
        this.orientation = orientation;
        this.min = (min == null) ? 0 : min.getMillis();
        this.max = (max == null) ? Long.MAX_VALUE : max.getMillis();
        this.frequency = frequency.getMillis();
        this.id = String.join("", EXPIRATION_JOB_PREFIX, UUID.randomUUID().toString());
    }

    public final String getId() {
        return id;
    }

    public final long getFrequency() {
        return frequency;
    }

    /**
     * The delay for the first notification, when the current time
     * is not in the valid time bracket for this callback
     * @param expirationDate license expiry date in milliseconds
     * @param now current time in milliseconds
     * @return time delay for the first notification
     */
    final TimeValue delay(long expirationDate, long now) {
        final TimeValue delay;
        switch (orientation) {
            case PRE:
                if (expirationDate >= now) {
                    // license not yet expired
                    long preExpiryDuration = expirationDate - now;
                    if (preExpiryDuration > max) {
                        // license duration is longer than maximum duration, delay it to the first match time
                        delay = TimeValue.timeValueMillis(preExpiryDuration - max);
                    } else if (preExpiryDuration <= max && preExpiryDuration >= min) {
                        // no delay in valid time bracket
                        delay = TimeValue.timeValueMillis(0);
                    } else {
                        // passed last match time
                        delay = null;
                    }
                } else {
                    // invalid after license expiry
                    delay = null;
                }
                break;
            case POST:
                if (expirationDate >= now) {
                    // license not yet expired, delay it to the first match time
                    delay = TimeValue.timeValueMillis(expirationDate - now + min);
                } else {
                    // license has expired
                    long expiredDuration = now - expirationDate;
                    if (expiredDuration < min) {
                        // license expiry duration is shorter than minimum duration, delay it to the first match time
                        delay = TimeValue.timeValueMillis(min - expiredDuration);
                    } else if (expiredDuration >= min && expiredDuration <= max) {
                        // no delay in valid time bracket
                        delay = TimeValue.timeValueMillis(0);
                    } else {
                        // passed last match time
                        delay = null;
                    }
                }
                break;
            default:
                throw new IllegalStateException("orientation [" + orientation + "] unknown");
        }
        return delay;
    }

    public final long nextScheduledTimeForExpiry(long expiryDate, long startTime, long time) {
        TimeValue delay = delay(expiryDate, time);
        if (delay != null) {
            long delayInMillis = delay.getMillis();
            if (delayInMillis == 0L) {
                if (startTime == time) {
                    // initial trigger and in time bracket, schedule immediately
                    return time;
                } else {
                    // in time bracket, add frequency
                    return time + frequency;
                }
            } else {
                // not in time bracket
                return time + delayInMillis;
            }
        }
        return -1;
    }

    /**
     * Code to execute when the expiry callback is triggered in a valid
     * time bracket
     * @param license license to operate on
     */
    public abstract void on(License license);

    public final String toString() {
        return LoggerMessageFormat.format(null, "ExpirationCallback:(orientation [{}],  min [{}], max [{}], freq [{}])",
                orientation.name(), TimeValue.timeValueMillis(min), TimeValue.timeValueMillis(max),
                TimeValue.timeValueMillis(frequency));
    }
}