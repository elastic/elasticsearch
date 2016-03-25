/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.license;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.AbstractLicenseeComponent;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.marvel.Marvel;

/**
 * {@code MarvelLicensee} determines whether certain features of Monitoring are enabled or disabled.
 * <p>
 * Once the license expires, the agent will stop:
 * <ul>
 * <li>Collecting and publishing new metrics.</li>
 * <li>Cleaning up (deleting) older indices.</li>
 * </ul>
 */
public class MarvelLicensee extends AbstractLicenseeComponent<MarvelLicensee> implements Licensee {

    @Inject
    public MarvelLicensee(Settings settings, LicenseeRegistry clientService) {
        super(settings, Marvel.NAME, clientService);
    }

    /**
     * {@inheritDoc}
     *
     * @see #collectionEnabled()
     * @see #cleaningEnabled()
     */
    @Override
    public String[] expirationMessages() {
        return new String[] {
                "The agent will stop collecting cluster and indices metrics",
                "The agent will stop automatically cleaning indices older than [xpack.monitoring.history.duration]",
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
                                            "[{}] licenses from within a single x-pack kibana instance. You will have to deploy a\n" +
                                            "separate and dedicated x-pack kibana instance for each [{}] cluster you wish to monitor.",
                                            newLicense.type(), newLicense.type(), newLicense.type()),
                                    LoggerMessageFormat.format(
                                            "Automatic index cleanup is disabled for clusters with [{}] license.", newLicense.type())

                            };
                    }
                }
        }
        return Strings.EMPTY_ARRAY;
    }

    /**
     * Determine if the index cleaning service is enabled.
     * <p>
     * Collection is only disabled <em>automatically</em> when the license expires/becomes invalid. Collection <em>can</em> be disabled
     * explicitly by the user, although that's generally a temporary solution to unrelated issues (e.g., initial setup when the monitoring
     * cluster doesn't actually exist).
     *
     * @return {@code true} as long as the license is valid. Otherwise {@code false}.
     */
    public boolean collectionEnabled() {
        // note: status is volatile, so don't do multiple checks without a local ref
        Status status = this.status;
        return status.getMode() != License.OperationMode.NONE && status.getLicenseState() != LicenseState.DISABLED;
    }

    /**
     * Determine if the index cleaning service is enabled.
     * <p>
     * Index cleaning is only disabled when the license expires/becomes invalid.
     *
     * @return {@code true} as long as the license is valid. Otherwise {@code false}.
     */
    public boolean cleaningEnabled() {
        // note: status is volatile, so don't do multiple checks without a local ref
        Status status = this.status;
        return status.getMode() != License.OperationMode.NONE && status.getLicenseState() != LicenseState.DISABLED;
    }

    /**
     * Determine if the current license allows the retention of indices to be modified.
     * <p>
     * Only users with the following license types can update the retention period:
     * <ul>
     * <li>{@link License.OperationMode#PLATINUM}</li>
     * <li>{@link License.OperationMode#GOLD}</li>
     * </ul>
     *
     * @return {@code true} if the user is allowed to modify the retention. Otherwise {@code false}.
     */
    public boolean allowUpdateRetention() {
        // note: status is volatile, so don't do multiple checks without a local ref
        Status status = this.status;
        return status.getMode() == License.OperationMode.PLATINUM || status.getMode() == License.OperationMode.GOLD;
    }
}
