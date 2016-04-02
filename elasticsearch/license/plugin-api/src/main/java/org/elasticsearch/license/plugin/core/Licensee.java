/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.License.OperationMode;

import java.util.Locale;

public interface Licensee {

    /**
     * Unique id used to log expiry and
     * acknowledgment messages
     */
    String id();

    /**
     * Messages to be printed when
     * logging license expiry warnings
     */
    String[] expirationMessages();

    /**
     * Messages to be returned when
     * installing <code>newLicense</code>
     * when <code>currentLicense</code> is
     * active
     */
    String[] acknowledgmentMessages(License currentLicense, License newLicense);

    /**
     * Notifies when a new license is activated
     * or when a license state change has occurred
     */
    void onChange(Status status);

    /**
     * {@code Status} represents both the type and state of a license.
     * <p>
     * Most places in the code are expected to use {@code volatile} {@code Status} fields. It's important to follow use a local reference
     * whenever checking different parts of the {@code Status}:
     * <pre>
     * Status status = this.status;
     * return status.getLicenseState() != LicenseState.DISABLED &amp;&amp;
     *        (status.getMode() == OperationMode.TRAIL || status.getMode == OperationMode.PLATINUM);
     * </pre>
     * Otherwise the license has the potential to change in-between both checks.
     */
    class Status {

        public static Status ENABLED = new Status(OperationMode.TRIAL, LicenseState.ENABLED);

        private final OperationMode mode;
        private final LicenseState licenseState;

        public Status(OperationMode mode, LicenseState licenseState) {
            this.mode = mode;
            this.licenseState = licenseState;
        }

        /**
         * Returns the operation mode of the license
         * responsible for the current <code>licenseState</code>
         * <p>
         * Note: Knowing the mode does not indicate whether the {@link #getLicenseState() state} is disabled. If that matters (e.g.,
         * disabling services when a license becomes disabled), then you should check it as well!
         */
        public OperationMode getMode() {
            return mode;
        }

        /**
         * When a license is active, the state is
         * {@link LicenseState#ENABLED}, upon license expiry
         * the state changes to {@link LicenseState#GRACE_PERIOD}
         * and after the grace period has ended the state changes
         * to {@link LicenseState#DISABLED}
         */
        public LicenseState getLicenseState() {
            return licenseState;
        }

        @Override
        public String toString() {
            switch (licenseState) {
                case DISABLED:
                    return "disabled " + mode.name().toLowerCase(Locale.ROOT);
                case GRACE_PERIOD:
                    return mode.name().toLowerCase(Locale.ROOT) + " grace period";
                default:
                    return mode.name().toLowerCase(Locale.ROOT);
            }
        }
    }
}
