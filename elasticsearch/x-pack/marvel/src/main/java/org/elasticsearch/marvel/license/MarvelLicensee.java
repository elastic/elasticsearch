/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.license;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.AbstractLicenseeComponent;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.marvel.MarvelPlugin;


public class MarvelLicensee extends AbstractLicenseeComponent<MarvelLicensee> implements Licensee {

    @Inject
    public MarvelLicensee(Settings settings, LicenseeRegistry clientService) {
        super(settings, MarvelPlugin.NAME, clientService);
    }

    @Override
    public String[] expirationMessages() {
        return new String[] {
                "The agent will stop collecting cluster and indices metrics",
                "The agent will stop to automatically clean up indices older than [marvel.history.duration]",
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
                            return new String[] {
                                    LoggerMessageFormat.format(
                                            "Multi-cluster support is disabled for clusters with [{}] license. If you are\n" +
                                            "running multiple clusters, users won't be able to access the clusters with\n" +
                                            "[{}] licenses from within a single Marvel instance. You will have to deploy a\n" +
                                            "separate and dedicated Marvel instance for each [{}] cluster you wish to monitor.",
                                            newLicense.type(), newLicense.type(), newLicense.type()),
                                    LoggerMessageFormat.format(
                                            "Automatic index cleanup is disabled for clusters with [{}] license.", newLicense.type())

                            };
                    }
                }
        }
        return Strings.EMPTY_ARRAY;
    }

    public boolean collectionEnabled() {
        // when checking multiple parts of the status, we should get a local reference to the status object since it is
        // volatile and can change between check statements...
        Status status = this.status;
        return status.getMode() != License.OperationMode.NONE &&
                status.getLicenseState() != LicenseState.DISABLED;
    }

    public boolean cleaningEnabled() {
        Status status = this.status;
        return status.getMode() != License.OperationMode.NONE &&
                status.getLicenseState() != LicenseState.DISABLED;
    }

    public boolean allowUpdateRetention() {
        Status status = this.status;
        return status.getMode() == License.OperationMode.PLATINUM || status.getMode() == License.OperationMode.GOLD;
    }
}
