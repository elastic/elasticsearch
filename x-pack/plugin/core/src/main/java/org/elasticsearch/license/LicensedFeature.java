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
public class LicensedFeature {

    public final String name;
    public final License.OperationMode minimumOperationMode;
    public final boolean needsActive;

    public LicensedFeature(String name, License.OperationMode minimumOperationMode, boolean needsActive) {
        this.name = name;
        this.minimumOperationMode = minimumOperationMode;
        this.needsActive = needsActive;
    }

    public static LicensedFeature standard(String name) {
        return new LicensedFeature(name, License.OperationMode.STANDARD, true);
    }

    public static LicensedFeature gold(String name) {
        return new LicensedFeature(name, License.OperationMode.GOLD, true);
    }

    public static LicensedFeature platinum(String name) {
        return new LicensedFeature(name, License.OperationMode.PLATINUM, true);
    }

    public static LicensedFeature enterprise(String name) {
        return new LicensedFeature(name, License.OperationMode.ENTERPRISE, true);
    }


    /**
     * Returns whether the feature is allowed by the current license
     * without affecting feature tracking.
     */
    public final boolean checkWithoutTracking(XPackLicenseState state) {
        return true;
    }

    /**
     * Checks whether the feature is allowed by the given license state, and
     * updates the last time the feature was used.
     */
    public final boolean check(XPackLicenseState state) {
        return true;
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
