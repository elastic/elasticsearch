/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.watcher.WatcherPlugin;

/**
 *
 */
public class LicenseService extends AbstractLifecycleComponent<LicenseService> implements Licensee {

    public static final String FEATURE_NAME = WatcherPlugin.NAME;

    private final LicenseeRegistry clientService;
    private volatile LicenseState state;

    @Inject
    public LicenseService(Settings settings, LicenseeRegistry clientService) {
        super(settings);
        this.clientService = clientService;
    }

    @Override
    public String id() {
        return FEATURE_NAME;
    }

    @Override
    public String[] expirationMessages() {
        // TODO add messages to be logged around license expiry
        return new String[0];
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

    @Override
    public void onChange(License license, LicenseState state) {
        synchronized (this) {
            this.state = state;
        }
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

    public boolean enabled() {
        return state != LicenseState.DISABLED;
    }
}
