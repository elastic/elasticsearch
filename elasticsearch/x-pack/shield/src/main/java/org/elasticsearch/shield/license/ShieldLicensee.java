/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.AbstractLicenseeComponent;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.shield.Shield;

/**
 *
 */
public class ShieldLicensee extends AbstractLicenseeComponent<ShieldLicensee> implements Licensee {

    private final boolean isTribeNode;
    private final ShieldLicenseState shieldLicenseState;

    @Inject
    public ShieldLicensee(Settings settings, LicenseeRegistry clientService, ShieldLicenseState shieldLicenseState) {
        super(settings, Shield.NAME, clientService);
        this.shieldLicenseState = shieldLicenseState;
        this.isTribeNode = settings.getGroups("tribe", true).isEmpty() == false;
    }

    @Override
    public void onChange(Status status) {
        super.onChange(status);
        shieldLicenseState.updateStatus(status);
    }

    @Override
    public String[] expirationMessages() {
        return new String[]{
                "Cluster health, cluster stats and indices stats operations are blocked",
                "All data operations (read and write) continue to work"
        };
    }

    @Override
    public String[] acknowledgmentMessages(License currentLicense, License newLicense) {
        switch (newLicense.operationMode()) {
            case BASIC:
                if (currentLicense != null) {
                    switch (currentLicense.operationMode()) {
                        case TRIAL:
                        case GOLD:
                        case PLATINUM:
                            return new String[]{"The following Shield functionality will be disabled: authentication, authorization, ip " +
                                    "filtering, auditing, SSL will be disabled on node restart. Please restart your node after applying " +
                                    "the license."};
                    }
                }
                break;
            case GOLD:
                if (currentLicense != null) {
                    switch (currentLicense.operationMode()) {
                        case TRIAL:
                        case BASIC:
                        case PLATINUM:
                            return new String[]{
                                    "Field and document level access control will be disabled",
                                    "Custom realms will be ignored"
                            };
                    }
                }
                break;
        }
        return Strings.EMPTY_ARRAY;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        // we rely on the initial licensee state to be enabled with trial operation mode
        // to ensure no operation is blocked due to not registering the licensee on a
        // tribe node
        if (!isTribeNode) {
            super.doStart();
        }
    }
}
