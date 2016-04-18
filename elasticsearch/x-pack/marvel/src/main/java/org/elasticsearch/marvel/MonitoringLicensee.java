/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.license.plugin.core.AbstractLicenseeComponent;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;

/**
 * {@code MarvelLicensee} determines whether certain features of Monitoring are enabled or disabled.
 * <p>
 * Once the license expires, the agent will stop:
 * <ul>
 * <li>Collecting and publishing new metrics.</li>
 * <li>Cleaning up (deleting) older indices.</li>
 * </ul>
 */
public class MonitoringLicensee extends AbstractLicenseeComponent<MonitoringLicensee> implements Licensee {

    @Inject
    public MonitoringLicensee(Settings settings, LicenseeRegistry clientService) {
        super(settings, Monitoring.NAME, clientService);
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
                        case STANDARD:
                        case GOLD:
                        case PLATINUM:
                            return new String[] {
                                LoggerMessageFormat.format(
                                        "Multi-cluster support is disabled for clusters with [{}] license. If you are\n" +
                                        "running multiple clusters, users won't be able to access the clusters with\n" +
                                        "[{}] licenses from within a single X-Pack Kibana instance. You will have to deploy a\n" +
                                        "separate and dedicated X-pack Kibana instance for each [{}] cluster you wish to monitor.",
                                        newLicense.type(), newLicense.type(), newLicense.type()),
                                LoggerMessageFormat.format(
                                        "Automatic index cleanup is locked to {} days for clusters with [{}] license.",
                                        MonitoringSettings.HISTORY_DURATION.getDefault(Settings.EMPTY).days(), newLicense.type())
                            };
                    }
                }
                break;
        }
        return Strings.EMPTY_ARRAY;
    }

    /**
     * Monitoring is always available regardless of the license type (operation mode)
     *
     * @return true
     */
    public boolean available() {
        return true;
    }

    /**
     * Determine if the index cleaning service is enabled.
     * <p>
     * Collection is only disabled <em>automatically</em> when the license expires. All modes are valid for collection.
     * <p>
     * Collection <em>can</em> be disabled explicitly by the user, although that's generally a temporary solution to unrelated issues
     * (e.g., initial setup when the monitoring cluster doesn't actually exist).
     *
     * @return {@code true} as long as the license is valid. Otherwise {@code false}.
     */
    public boolean collectionEnabled() {
        return status.getLicenseState() != LicenseState.DISABLED;
    }

    /**
     * Determine if the index cleaning service is enabled.
     * <p>
     * Index cleaning is only disabled when the license expires. All modes are valid for cleaning.
     *
     * @return {@code true} as long as the license is valid. Otherwise {@code false}.
     */
    public boolean cleaningEnabled() {
        return status.getLicenseState() != LicenseState.DISABLED;
    }

    /**
     * Determine if the current license allows the retention of indices to be modified.
     * <p>
     * Only users with a non-{@link OperationMode#BASIC} license can update the retention period.
     * <p>
     * Note: This does not consider the <em>state</em> of the license so that any change is remembered for when they fix their license.
     *
     * @return {@code true} if the user is allowed to modify the retention. Otherwise {@code false}.
     */
    public boolean allowUpdateRetention() {
        return status.getMode() != OperationMode.BASIC;
    }
}
