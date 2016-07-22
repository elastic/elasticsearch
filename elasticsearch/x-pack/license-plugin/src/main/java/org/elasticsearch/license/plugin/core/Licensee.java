/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.license.core.License.OperationMode;

import java.util.Locale;
import java.util.Objects;

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
     * changing from current operation mode
     * to new operation mode
     */
    String[] acknowledgmentMessages(OperationMode currentMode, OperationMode newMode);

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
     * return status.isActive() &amp;&amp;
     *        (status.getMode() == OperationMode.TRIAL || status.getMode == OperationMode.PLATINUM);
     * </pre>
     * Otherwise the license has the potential to change in-between both checks.
     */
    class Status {

        public static Status ENABLED = new Status(OperationMode.TRIAL, true);
        public static Status MISSING = new Status(OperationMode.MISSING, false);

        private final OperationMode mode;
        private final boolean active;

        public Status(OperationMode mode, boolean active) {
            this.mode = mode;
            this.active = active;
        }

        /**
         * Returns the operation mode of the license
         * responsible for the current <code>licenseState</code>
         * <p>
         * Note: Knowing the mode does not indicate whether the license is active. If that matters (e.g.,
         * disabling services when a license becomes disabled), then check {@link #isActive()}.
         */
        public OperationMode getMode() {
            return mode;
        }

        /** Returns true if the license is within the issue date and grace period, or false otherwise */
        public boolean isActive() {
            return active;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return active == status.active &&
                mode == status.mode;
        }

        @Override
        public int hashCode() {
            return Objects.hash(mode, active);
        }

        @Override
        public String toString() {
            if (active) {
                return mode.name().toLowerCase(Locale.ROOT);
            } else {
                return "disabled " + mode.name().toLowerCase(Locale.ROOT);
            }
        }
    }
}
