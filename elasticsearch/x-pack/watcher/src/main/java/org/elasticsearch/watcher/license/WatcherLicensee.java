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
import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.license.plugin.core.AbstractLicenseeComponent;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.watcher.Watcher;

import static org.elasticsearch.license.core.License.OperationMode.TRIAL;
import static org.elasticsearch.license.core.License.OperationMode.GOLD;
import static org.elasticsearch.license.core.License.OperationMode.PLATINUM;

public class WatcherLicensee extends AbstractLicenseeComponent<WatcherLicensee> {

    public static final String ID = Watcher.NAME;

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
                        case STANDARD:
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

    /**
     * Determine if Watcher should be enabled.
     * <p>
     * Watcher is only disabled when the license has expired or if the mode is not:
     * <ul>
     * <li>{@link OperationMode#PLATINUM}</li>
     * <li>{@link OperationMode#GOLD}</li>
     * <li>{@link OperationMode#STANDARD}</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     *
     * @return {@code true} as long as the license is valid. Otherwise {@code false}.
     */
    public boolean isWatcherTransportActionAllowed() {
        // status is volatile, so a local variable is used for a consistent view
        Status localStatus = status;

        return localStatus.getLicenseState() != LicenseState.DISABLED && localStatus.getMode() != OperationMode.BASIC;
    }
}
