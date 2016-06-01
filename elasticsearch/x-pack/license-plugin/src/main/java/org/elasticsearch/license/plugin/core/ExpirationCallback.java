/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;

public abstract class ExpirationCallback {

    public enum Orientation {PRE, POST}

    public static abstract class Pre extends ExpirationCallback {

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
        public TimeValue delay(long expiryDuration) {
            return TimeValue.timeValueMillis(expiryDuration - max.getMillis());
        }
    }

    public static abstract class Post extends ExpirationCallback {

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
        public TimeValue delay(long expiryDuration) {
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

    protected final Orientation orientation;
    protected final TimeValue min;
    protected final TimeValue max;
    private final TimeValue frequency;

    private ExpirationCallback(Orientation orientation, TimeValue min, TimeValue max, TimeValue frequency) {
        this.orientation = orientation;
        this.min = (min == null) ? TimeValue.timeValueMillis(0) : min;
        this.max = (max == null) ? TimeValue.timeValueMillis(Long.MAX_VALUE) : max;
        this.frequency = frequency;
    }

    public TimeValue frequency() {
        return frequency;
    }

    public abstract TimeValue delay(long expiryDuration);

    public abstract boolean matches(long expirationDate, long now);

    public abstract void on(License license);

    @Override
    public String toString() {
        return LoggerMessageFormat.format(null, "ExpirationCallback:(orientation [{}],  min [{}], max [{}], freq [{}])",
                orientation.name(), min, max, frequency);
    }
}
