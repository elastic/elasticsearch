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
     * @return true if the license allows for the stats and health APIs to be used.
     */
    public boolean statsAndHealthEnabled() {
        return status.getLicenseState() != LicenseState.DISABLED;
    }

    /**
     * Determine if Document Level Security (DLS) and Field Level Security (FLS) should be enabled.
     * <p>
     * DLS and FLS are only disabled when the mode is not:
     * <ul>
     * <li>{@link OperationMode#PLATINUM}</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     * Note: This does not consider the <em>state</em> of the license so that Security does not suddenly leak information!
     *
     * @return {@code true} to enable DLS and FLS. Otherwise {@code false}.
     */
    public boolean documentAndFieldLevelSecurityEnabled() {
        Status status = this.status;
        return status.getMode() == OperationMode.TRIAL || status.getMode() == OperationMode.PLATINUM;
    }

    /**
     * Determine if Custom Realms should be enabled.
     * <p>
     * Custom Realms are only disabled when the mode is not:
     * <ul>
     * <li>{@link OperationMode#PLATINUM}</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     * Note: This does not consider the <em>state</em> of the license so that Security does not suddenly block requests!
     *
     * @return {@code true} to enable Custom Realms. Otherwise {@code false}.
     */
    public boolean customRealmsEnabled() {
        Status status = this.status;
        return status.getMode() == OperationMode.TRIAL || status.getMode() == OperationMode.PLATINUM;
    }

    void updateStatus(Status status) {
        this.status = status;
    }
}
