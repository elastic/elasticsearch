/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.mode.Mode;


public class LicenseService extends AbstractLifecycleComponent<LicenseService> implements Licensee {

    public static final String FEATURE_NAME = MarvelPlugin.NAME;

    private final LicensesManagerService managerService;
    private final LicenseeRegistry clientService;
    private final MarvelSettings marvelSettings;

    private volatile Mode mode;
    private volatile LicenseState state;
    private volatile long expiryDate;

    @Inject
    public LicenseService(Settings settings, LicenseeRegistry clientService, LicensesManagerService managerService, MarvelSettings marvelSettings) {
        super(settings);
        this.managerService = managerService;
        this.clientService = clientService;
        this.marvelSettings = marvelSettings;
        this.mode = Mode.LITE;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        clientService.register(this);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    /**
     * @return the current marvel's operating mode
     */
    public Mode mode() {
        return mode;
    }

    /**
     * @return all registered licenses
     */
    public License license() {
        return managerService.getLicense();
    }

    /**
     * @return true if the marvel license is enabled
     */
    public boolean enabled() {
        return state == LicenseState.ENABLED || state == LicenseState.GRACE_PERIOD;
    }

    /**
     * TODO: remove licensing grace period, just check for state == LicensesClientService.LicenseState.GRACE_PERIOD instead
     *
     * @return true if marvel is running within the "grace period", ie when the license
     * is expired but a given extra delay is not yet elapsed
     */
    public boolean inExpirationGracePeriod() {
        return System.currentTimeMillis() <= (expiryDate() + marvelSettings.licenseExpirationGracePeriod().millis());
    }

    /**
     * @return the license's expiration date (as a long)
     */
    public long expiryDate() {
        return expiryDate;
    }

    @Override
    public String id() {
        return FEATURE_NAME;
    }

    @Override
    public String[] expirationMessages() {
        // TODO add messages to be logged around license expiry
        return Strings.EMPTY_ARRAY;
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
                                            "Multi-cluster support is disabled for clusters with [{}] licenses.\n" +
                                                    "If you are running multiple customers, users won't be able to access this\n" +
                                                    "all the clusters with [{}] licenses from a single Marvel instance. To access them\n" +
                                                    "a dedicated and separated marvel instance will be required for each cluster",
                                            newLicense.type(), newLicense.type())
                            };
                    }
                }
        }
        return Strings.EMPTY_ARRAY;
    }

    @Override
    public void onChange(License license, LicenseState state) {
        synchronized (this) {
            this.state = state;
            if (license != null) {
                try {
                    mode = Mode.fromName(license.type());
                } catch (IllegalArgumentException e) {
                    mode = Mode.LITE;
                }
                expiryDate = license.expiryDate();
            } else {
                mode = Mode.LITE;
            }
            if (state == LicenseState.DISABLED) {
                mode = Mode.LITE;
            }
        }
    }
}
