/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.license;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.AbstractLicenseeComponent;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.watcher.WatcherPlugin;

import static org.elasticsearch.license.core.License.OperationMode.GOLD;
import static org.elasticsearch.license.core.License.OperationMode.PLATINUM;
import static org.elasticsearch.license.core.License.OperationMode.TRIAL;

public class WatcherLicensee extends AbstractLicenseeComponent<WatcherLicensee> {

    public static final String ID = WatcherPlugin.NAME;

    @Inject
    public WatcherLicensee(Settings settings, LicenseeRegistry clientService) {
        super(settings, ID, clientService);
    }

    @Override
    public String[] expirationMessages() {
        return new String[] {
                "PUT / GET watch APIs are disabled, DELETE watch API continues to work",
                "Watches execute and write to the history",
                "The actions of the watches don't execute"
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
                            return new String[] { "Watcher will be disabled" };
                    }
                }
                break;
        }
        return Strings.EMPTY_ARRAY;
    }

    public boolean isExecutingActionsAllowed() {
        return isPutWatchAllowed();
    }

    public boolean isGetWatchAllowed() {
        return isPutWatchAllowed();
    }

    public boolean isPutWatchAllowed() {
        return isWatcherTransportActionAllowed();
    }

    public boolean isWatcherTransportActionAllowed() {
        Status localStatus = status;
        boolean isLicenseStateActive = localStatus.getLicenseState() != LicenseState.DISABLED;
        License.OperationMode operationMode = localStatus.getMode();
        return isLicenseStateActive && (operationMode == TRIAL || operationMode == GOLD || operationMode == PLATINUM);
    }
}
