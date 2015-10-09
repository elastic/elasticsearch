/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.shield.ShieldPlugin;

/**
 *
 */
public class LicenseService extends AbstractLifecycleComponent<LicenseService> implements Licensee {

    public static final String FEATURE_NAME = ShieldPlugin.NAME;

    private final LicenseeRegistry licenseeRegistry;
    private final LicenseEventsNotifier notifier;

    private volatile LicenseState state = LicenseState.DISABLED;

    @Inject
    public LicenseService(Settings settings, LicenseeRegistry licenseeRegistry, LicenseEventsNotifier notifier) {
        super(settings);
        this.licenseeRegistry = licenseeRegistry;
        this.notifier = notifier;
    }

    @Override
    public String id() {
        return FEATURE_NAME;
    }

    @Override
    public String[] expirationMessages() {
        return new String[] {
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
                            return new String[] { "The following Shield functionality will be disabled: authentication, authorization, ip filtering, auditing, SSL will be disabled on node restart. Please restart your node after applying the license." };
                    }
                }
                break;
            case GOLD:
                if (currentLicense != null) {
                    switch (currentLicense.operationMode()) {
                        case TRIAL:
                        case BASIC:
                        case PLATINUM:
                            return new String[] {
                                    "Field and document level access control will be disabled"
                            };
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
            notifier.notify(state);
        }
    }
    public LicenseState state() {
        return state;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        if (settings.getGroups("tribe", true).isEmpty()) {
            licenseeRegistry.register(this);
        } else {
            //TODO currently we disable licensing on tribe node. remove this once es core supports merging cluster
            onChange(null,  LicenseState.ENABLED);
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }
}
