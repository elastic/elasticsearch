/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.license.LicenseStateListener;
import org.elasticsearch.license.XPackLicenseState;

import java.util.Objects;

/**
 * A listener for license state changes that provides log messages when a license change
 * causes security to switch between enable and disabled (or vice versa).
 */
public class SecurityStatusChangeListener implements LicenseStateListener {

    private final Logger logger;
    private final XPackLicenseState licenseState;
    private Boolean securityEnabled;

    public SecurityStatusChangeListener(XPackLicenseState licenseState) {
        this.logger = LogManager.getLogger(getClass());
        this.licenseState = licenseState;
        this.securityEnabled = null;
    }

    /**
     * This listener will not be registered if security has been explicitly disabled, so we only need to account for dynamic changes due
     * to changes in the applied license.
     */
    @Override
    public synchronized void licenseStateChanged() {
        final boolean newState = licenseState.isSecurityAvailable() && licenseState.isSecurityDisabledByLicenseDefaults() == false;
        // old state might be null (undefined) so do Object comparison
        if (Objects.equals(newState, securityEnabled) == false) {
            logger.info("Active license is now [{}]; Security is {}", licenseState.getOperationMode(), newState ? "enabled" : "disabled");
            this.securityEnabled = newState;
        }
    }
}
