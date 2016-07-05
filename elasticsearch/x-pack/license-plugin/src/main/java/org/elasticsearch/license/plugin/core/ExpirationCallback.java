/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.xpack.scheduler.SchedulerEngine;

import java.util.UUID;

public abstract class ExpirationCallback {

    final static String EXPIRATION_JOB_PREFIX = ".license_expiration_job_";

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

        @Override
        public boolean matches(long expirationDate, long now) {
            long expiryDuration = expirationDate - now;
            if (expiryDuration > 0L) {
                if (expiryDuration <= max.getMillis()) {
                    return expiryDuration >= min.getMillis();
                }
            }
            return false;
        }

        @Override
        public TimeValue delay(long expirationDate, long now) {
            return TimeValue.timeValueMillis((expirationDate - now) - max.getMillis());
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

        @Override
        public boolean matches(long expirationDate, long now) {
            long postExpiryDuration = now - expirationDate;
            if (postExpiryDuration > 0L) {
                if (postExpiryDuration <= max.getMillis()) {
                    return postExpiryDuration >= min.getMillis();
                }
            }
            return false;
        }

        @Override
        public TimeValue delay(long expirationDate, long now) {
            long expiryDuration = expirationDate - now;
            final long delay;
            if (expiryDuration >= 0L) {
                delay = expiryDuration + min.getMillis();
            } else {
                delay = (-1L * expiryDuration) - min.getMillis();
            }
            if (delay > 0L) {
                return TimeValue.timeValueMillis(delay);
            } else {
                return null;
            }
        }
    }

    private final String id;
    protected final Orientation orientation;
    protected final TimeValue min;
    protected final TimeValue max;
    private final TimeValue frequency;

    private ExpirationCallback(Orientation orientation, TimeValue min, TimeValue max, TimeValue frequency) {
        this.orientation = orientation;
        this.min = (min == null) ? TimeValue.timeValueMillis(0) : min;
        this.max = (max == null) ? TimeValue.timeValueMillis(Long.MAX_VALUE) : max;
        this.frequency = frequency;
        this.id = String.join("", EXPIRATION_JOB_PREFIX, UUID.randomUUID().toString());
    }

    public String getId() {
        return id;
    }

    public TimeValue frequency() {
        return frequency;
    }

    public abstract TimeValue delay(long expirationDate, long now);

    public abstract boolean matches(long expirationDate, long now);

    public abstract void on(License license);

    public SchedulerEngine.Schedule schedule(long expiryDate) {
        return new ExpirySchedule(expiryDate);
    }

    public String toString() {
        return LoggerMessageFormat.format(null, "ExpirationCallback:(orientation [{}],  min [{}], max [{}], freq [{}])",
                orientation.name(), min, max, frequency);
    }

    private class ExpirySchedule implements SchedulerEngine.Schedule {

        private final long expiryDate;

        private ExpirySchedule(long expiryDate) {
            this.expiryDate = expiryDate;
        }

        @Override
        public long nextScheduledTimeAfter(long startTime, long time) {
            if (matches(expiryDate, time)) {
                if (startTime == time) {
                    return time;
                } else {
                    return time + frequency().getMillis();
                }
            } else {
                if (startTime == time) {
                    final TimeValue delay = delay(expiryDate, time);
                    if (delay != null) {
                        return time + delay.getMillis();
                    }
                }
                return -1;
            }
        }
    }
}