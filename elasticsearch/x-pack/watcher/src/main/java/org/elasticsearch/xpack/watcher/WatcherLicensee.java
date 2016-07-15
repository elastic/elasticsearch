/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.license.plugin.core.AbstractLicenseeComponent;
import org.elasticsearch.license.plugin.core.LicenseState;

public class WatcherLicensee extends AbstractLicenseeComponent {

    public static final String ID = Watcher.NAME;

    public WatcherLicensee(Settings settings) {
        super(settings, ID);
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

    /**
     * Determine if Watcher is available based on the current license.
     * <p>
     * Watcher is available if the license is active (hasn't expired) and of one of the following types:
     * <ul>
     * <li>{@link OperationMode#PLATINUM}</li>
     * <li>{@link OperationMode#GOLD}</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     *
     * @return {@code true} as long as the license is valid. Otherwise {@code false}.
     */
    public boolean isAvailable() {
        // status is volatile, so a local variable is used for a consistent view
        Status localStatus = status;

        if (localStatus.getLicenseState() == LicenseState.DISABLED) {
            return false;
        }

        switch (localStatus.getMode()) {
            case TRIAL:
            case GOLD:
            case PLATINUM:
                return true;
            default:
                return false;
        }
    }

    public boolean isExecutingActionsAllowed() {
        return isWatcherTransportActionAllowed();
    }

    public boolean isGetWatchAllowed() {
        return isWatcherTransportActionAllowed();
    }

    public boolean isPutWatchAllowed() {
        return isWatcherTransportActionAllowed();
    }


    public boolean isWatcherTransportActionAllowed() {
        return isAvailable();
    }


}
