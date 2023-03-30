/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license.mutable;

import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseStateListener;
import org.elasticsearch.license.XPackLicenseState;

import java.util.function.LongSupplier;

public class MutableXPackLicenseState extends XPackLicenseState {

    public MutableXPackLicenseState(LongSupplier epochMillisProvider) {
        super(epochMillisProvider);
    }

    /**
     * Updates the current state of the license, which will change what features are available.
     *
     * @param mode   The mode (type) of the current license.
     * @param active True if the current license exists and is within its allowed usage period; false if it is expired or missing.
     * @param expiryWarning Warning to emit on license checks about the license expiring soon.
     */
    public void update(License.OperationMode mode, boolean active, String expiryWarning) {
        status = new Status(mode, active, expiryWarning);
        listeners.forEach(LicenseStateListener::licenseStateChanged);
    }

}
