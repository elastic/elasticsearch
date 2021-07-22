/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import java.util.Objects;

/**
 * A base class for checking licensed features against the license.
 */
public abstract class LicensedFeature {

    public static class Momentary extends LicensedFeature {

        private Momentary(String name, License.OperationMode minimumOperationMode, boolean needsActive) {
            super(name, minimumOperationMode, needsActive);
        }

        /**
         * Checks whether the feature is allowed by the given license state, and
         * updates the last time the feature was used.
         */
        public boolean check(XPackLicenseState state) {
            if (state.isAllowed(this)) {
                state.featureUsed(this);
                return true;
            } else {
                return false;
            }
        }
    }

    public static class Persistent extends LicensedFeature {
        private Persistent(String name, License.OperationMode minimumOperationMode, boolean needsActive) {
            super(name, minimumOperationMode, needsActive);
        }

        public boolean checkAndStartTracking(XPackLicenseState state, String contextName) {
            if (state.isAllowed(this)) {
                state.enableUsageTracking(this, contextName);
                return true;
            } else {
                return false;
            }
        }

        public void stopTracking(XPackLicenseState state, String contextName) {
            state.disableUsageTracking(this, contextName);
        }
    }

    public static class Untracked extends LicensedFeature {
        private Untracked(String name, License.OperationMode minimumOperationMode, boolean needsActive) {
            super(name, minimumOperationMode, needsActive);
        }

        public boolean check(XPackLicenseState state) {
            return state.isAllowed(this);
        }
    }

    final String name;
    final License.OperationMode minimumOperationMode;
    final boolean needsActive;

    public LicensedFeature(String name, License.OperationMode minimumOperationMode, boolean needsActive) {
        this.name = name;
        this.minimumOperationMode = minimumOperationMode;
        this.needsActive = needsActive;
    }

    /**
     * Creates a feature that is tracked at the moment it is checked.
     * @param name A unique name for the feature that will be returned in
     *             the tracking API. This should not change.
     * @param licenseLevel The lowest level of license in which this feature should be allowed.
     */
    public static Momentary momentary(String name, License.OperationMode licenseLevel) {
        return new Momentary(name, licenseLevel, true);
    }

    public static Persistent persistent(String name, License.OperationMode licenseLevel) {
        return new Persistent(name, licenseLevel, true);
    }

    /**
     * Creates a feature that is tracked at the moment it is checked, but that is lenient as
     * to whether the license needs to be active to allow the feature.
     */
    @Deprecated
    public static Momentary momentaryLenient(String name, License.OperationMode licenseLevel) {
        return new Momentary(name, licenseLevel, false);
    }

    @Deprecated
    public static Persistent persistentLenient(String name, License.OperationMode licenseLevel) {
        return new Persistent(name, licenseLevel, false);
    }

    /**
     * Returns whether the feature is allowed by the current license
     * without affecting feature tracking.
     */
    public final boolean checkWithoutTracking(XPackLicenseState state) {
        return state.isAllowed(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LicensedFeature that = (LicensedFeature) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
