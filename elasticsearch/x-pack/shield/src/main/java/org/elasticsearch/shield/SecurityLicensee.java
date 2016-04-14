/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.AbstractLicenseeComponent;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;

/**
 *
 */
public class SecurityLicensee extends AbstractLicenseeComponent<SecurityLicensee> implements Licensee {

    private final boolean isTribeNode;
    private final SecurityLicenseState shieldLicenseState;

    @Inject
    public SecurityLicensee(Settings settings, LicenseeRegistry clientService, SecurityLicenseState shieldLicenseState) {
        super(settings, Security.NAME, clientService);
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
                        case STANDARD:
                        case GOLD:
                        case PLATINUM:
                            return new String[] {
                                "The following X-Pack security functionality will be disabled: authentication, authorization, " +
                                "ip filtering, and auditing. Please restart your node after applying the license.",
                                "Field and document level access control will be disabled.",
                                "Custom realms will be ignored."
                            };
                    }
                }
                break;
            case GOLD:
                if (currentLicense != null) {
                    switch (currentLicense.operationMode()) {
                        case BASIC:
                        case STANDARD:
                        // ^^ though technically it was already disabled, it's not bad to remind them
                        case TRIAL:
                        case PLATINUM:
                            return new String[] {
                                "Field and document level access control will be disabled.",
                                "Custom realms will be ignored."
                            };
                    }
                }
                break;
            case STANDARD:
                if (currentLicense != null) {
                    switch (currentLicense.operationMode()) {
                        case BASIC:
                        // ^^ though technically it was already disabled, it's not bad to remind them
                        case GOLD:
                        case PLATINUM:
                        case TRIAL:
                            return new String[] {
                                    "Authentication will be limited to the native realms.",
                                    "IP filtering and auditing will be disabled.",
                                    "Field and document level access control will be disabled.",
                                    "Custom realms will be ignored."
                            };
                    }
                }
        }
        return Strings.EMPTY_ARRAY;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        // we rely on the initial licensee state to be enabled with trial operation mode
        // to ensure no operation is blocked due to not registering the licensee on a
        // tribe node
        if (isTribeNode == false) {
            super.doStart();
        }
    }
}
