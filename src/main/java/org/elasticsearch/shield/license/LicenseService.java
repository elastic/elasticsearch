/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.shield.ShieldPlugin;

/**
 *
 */
public class LicenseService extends AbstractLifecycleComponent<LicenseService> {

    public static final String FEATURE_NAME = ShieldPlugin.NAME;

    private static final LicensesService.TrialLicenseOptions TRIAL_LICENSE_OPTIONS =
            new LicensesService.TrialLicenseOptions(TimeValue.timeValueHours(30 * 24), 1000);

    private final LicensesClientService licensesClientService;
    private final LicenseEventsNotifier notifier;

    private boolean enabled = false;

    @Inject
    public LicenseService(Settings settings, LicensesClientService licensesClientService, LicenseEventsNotifier notifier) {
        super(settings);
        this.licensesClientService = licensesClientService;
        this.notifier = notifier;
    }

    public synchronized boolean enabled() {
        return enabled;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        licensesClientService.register(FEATURE_NAME, TRIAL_LICENSE_OPTIONS, new InternalListener());
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    class InternalListener implements LicensesClientService.Listener {

        @Override
        public void onEnabled() {
            synchronized (LicenseService.this) {
                enabled = true;
                notifier.notifyEnabled();
            }
        }

        @Override
        public void onDisabled() {
            synchronized (LicenseService.this) {
                enabled = false;
                notifier.notifyDisabled();
            }
        }
    }
}
