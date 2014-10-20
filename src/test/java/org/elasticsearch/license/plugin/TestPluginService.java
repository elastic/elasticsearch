/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.license.plugin.core.LicensesService;

import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class TestPluginService extends AbstractLifecycleComponent<TestPluginService> {

    private LicensesClientService licensesClientService;

    // should be the same string used by the license Manger to generate
    // signed license
    static final String FEATURE_NAME = "shield";

    // specify the trial license spec for the feature
    // example: 30 day trial on 1000 nodes
    final LicensesService.TrialLicenseOptions trialLicenseOptions = new LicensesService.TrialLicenseOptions(30, 1000);

    private AtomicBoolean enabled = new AtomicBoolean(false);

    @Inject
    public TestPluginService(Settings settings, LicensesClientService licensesClientService) {
        super(settings);
        this.licensesClientService = licensesClientService;
    }

    // check if feature is enabled
    public boolean enabled() {
                                   return enabled.get();
                                                                                       }

    protected void doStart() throws ElasticsearchException {
        licensesClientService.register(FEATURE_NAME,
                trialLicenseOptions,
                new LicensingClientListener());
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    private class LicensingClientListener implements LicensesClientService.Listener {

        @Override
        public void onEnabled() {
                                      enabled.set(true);
                                                                                      }

        @Override
        public void onDisabled() {
                                       enabled.set(false);
                                                                                         }
    }
}
