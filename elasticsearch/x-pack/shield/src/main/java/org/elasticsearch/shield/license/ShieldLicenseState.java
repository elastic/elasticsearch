/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.license;

import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee.Status;


/**
 * This class serves to decouple shield code that needs to check the license state from the {@link ShieldLicensee} as the
 * tight coupling causes issues with guice injection and circular dependencies
 */
public class ShieldLicenseState {

    // we initialize the licensee status to enabled with trial operation mode to ensure no
    // legitimate requests are blocked before initial license plugin notification
    protected volatile Status status = Status.ENABLED;

    /**
     * @return true if the license allows for security features to be enabled (authc, authz, ip filter, audit, etc)
     */
    public boolean securityEnabled() {
        return status.getMode() != OperationMode.BASIC;
    }

    /**
     * Indicates whether the stats and health API calls should be allowed. If a license is expired and past the grace
     * period then we deny these calls.
     *
     * @return true if the license allows for the stats and health apis to be used.
     */
    public boolean statsAndHealthEnabled() {
        return status.getLicenseState() != LicenseState.DISABLED;
    }

    /**
     * @return true if the license enables DLS and FLS
     */
    public boolean documentAndFieldLevelSecurityEnabled() {
        Status status = this.status;
        return status.getMode() == OperationMode.PLATINUM || status.getMode() == OperationMode.TRIAL;
    }

    /**
     * @return true if the license enables the use of custom authentication realms
     */
    public boolean customRealmsEnabled() {
        Status status = this.status;
        return status.getMode() == OperationMode.PLATINUM || status.getMode() == OperationMode.TRIAL;
    }

    void updateStatus(Status status) {
        this.status = status;
    }
}
