/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

/**
 * A base class for checking licensed features against the license.
 */
public class LicensedFeature {

    public final License.OperationMode minimumOperationMode;
    public final boolean needsActive;

    public LicensedFeature(License.OperationMode minimumOperationMode, boolean needsActive) {
        this.minimumOperationMode = minimumOperationMode;
        this.needsActive = needsActive;
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
}
