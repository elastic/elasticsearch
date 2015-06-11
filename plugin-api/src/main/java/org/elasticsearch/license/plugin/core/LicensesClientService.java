/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;

import java.util.Collection;


//@ImplementedBy(LicensesService.class)
public interface LicensesClientService {

    public interface Listener {

        /**
         * Called to enable a feature
         */
        public void onEnabled(License license);

        /**
         * Called to disable a feature
         */
        public void onDisabled(License license);

    }

    /**
     * Registers a feature for licensing
     *
     * @param feature             - name of the feature to register (must be in sync with license Generator feature name)
     * @param trialLicenseOptions - Trial license specification used to generate a one-time trial license for the feature;
     *                            use <code>null</code> if no trial license should be generated for the feature
     * @param expirationCallbacks - A collection of Pre and/or Post expiration callbacks
     * @param listener            - used to notify on feature enable/disable
     */
    void register(String feature, TrialLicenseOptions trialLicenseOptions, Collection<ExpirationCallback> expirationCallbacks, Listener listener);
    
    public static class TrialLicenseOptions {
        final TimeValue duration;
        final int maxNodes;

        public TrialLicenseOptions(TimeValue duration, int maxNodes) {
            this.duration = duration;
            this.maxNodes = maxNodes;
        }
    }
    

    public static interface LicenseCallback {
        void on(License license, ExpirationStatus status);
    }

    public static abstract class ExpirationCallback implements LicenseCallback {

        public enum Orientation { PRE, POST }

        public static abstract class Pre extends ExpirationCallback {

            /**
             * Callback schedule prior to license expiry
             *
             * @param min latest relative time to execute before license expiry
             * @param max earliest relative time to execute before license expiry
             * @param frequency interval between execution
             */
            public Pre(TimeValue min, TimeValue max, TimeValue frequency) {
                super(Orientation.PRE, min, max, frequency);
            }

            @Override
            public boolean matches(long expirationDate, long now) {
                long expiryDuration = expirationDate - now;
                if (expiryDuration > 0l) {
                    if (expiryDuration <= max().getMillis()) {
                        return expiryDuration >= min().getMillis();
                    }
                }
                return false;
            }
        }

        public static abstract class Post extends ExpirationCallback {

            /**
             * Callback schedule after license expiry
             *
             * @param min earliest relative time to execute after license expiry
             * @param max latest relative time to execute after license expiry
             * @param frequency interval between execution
             */
            public Post(TimeValue min, TimeValue max, TimeValue frequency) {
                super(Orientation.POST, min, max, frequency);
            }

            @Override
            public boolean matches(long expirationDate, long now) {
                long postExpiryDuration = now - expirationDate;
                if (postExpiryDuration > 0l) {
                    if (postExpiryDuration <= max().getMillis()) {
                        return postExpiryDuration >= min().getMillis();
                    }
                }
                return false;
            }
        }

        private final Orientation orientation;
        private final TimeValue min;
        private final TimeValue max;
        private final TimeValue frequency;

        private ExpirationCallback(Orientation orientation, TimeValue min, TimeValue max, TimeValue frequency) {
            this.orientation = orientation;
            this.min = (min == null) ? TimeValue.timeValueMillis(0) : min;
            this.max = (max == null) ? TimeValue.timeValueMillis(Long.MAX_VALUE) : max;
            this.frequency = frequency;
            if (frequency == null) {
                throw new IllegalArgumentException("frequency can not be null");
            }
        }

        public Orientation orientation() {
            return orientation;
        }

        public TimeValue min() {
            return min;
        }

        public TimeValue max() {
            return max;
        }

        public TimeValue frequency() {
            return frequency;
        }

        public abstract boolean matches(long expirationDate, long now);
    }
    
    public static class ExpirationStatus {
        private final boolean expired;
        private final TimeValue time;

        ExpirationStatus(boolean expired, TimeValue time) {
            this.expired = expired;
            this.time = time;
        }

        public boolean expired() {
            return expired;
        }

        public TimeValue time() {
            return time;
        }
    }


}
