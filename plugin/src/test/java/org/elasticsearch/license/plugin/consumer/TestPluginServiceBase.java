/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.consumer;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.license.plugin.core.LicensesService;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class TestPluginServiceBase extends AbstractLifecycleComponent<TestPluginServiceBase> implements ClusterStateListener {

    private LicensesClientService licensesClientService;

    private final ClusterService clusterService;
    // specify the trial license spec for the feature
    // example: 30 day trial on 1000 nodes
    final LicensesService.TrialLicenseOptions trialLicenseOptions;

    final boolean eagerLicenseRegistration;

    public final AtomicBoolean registered = new AtomicBoolean(false);

    private volatile AtomicBoolean enabled = new AtomicBoolean(false);

    public TestPluginServiceBase(boolean eagerLicenseRegistration, Settings settings, LicensesClientService licensesClientService, ClusterService clusterService) {
        super(settings);
        this.eagerLicenseRegistration = eagerLicenseRegistration;
        this.licensesClientService = licensesClientService;
        int durationInSec = settings.getAsInt(settingPrefix() + ".trial_license_duration_in_seconds", -1);
        if (durationInSec == -1) {
            this.trialLicenseOptions = null;
        } else {
            this.trialLicenseOptions = new LicensesService.TrialLicenseOptions(TimeValue.timeValueSeconds(durationInSec), 1000);
        }
        if (!eagerLicenseRegistration) {
            this.clusterService = clusterService;
            clusterService.add(this);
        } else {
            this.clusterService = null;
        }
    }

    // should be the same string used by the license Manger to generate
    // signed license
    public abstract String featureName();

    public abstract String settingPrefix();

    // check if feature is enabled
    public boolean enabled() {
        return enabled.get();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!eagerLicenseRegistration && !event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            if (registered.compareAndSet(false, true)) {
                logger.info("Registering to licensesService [lazy]");
                licensesClientService.register(featureName(),
                        trialLicenseOptions,
                        new LicensingClientListener());
            }
        }
    }

    protected void doStart() throws ElasticsearchException {
        if (eagerLicenseRegistration) {
            if (registered.compareAndSet(false, true)) {
                logger.info("Registering to licensesService [eager]");
                licensesClientService.register(featureName(),
                        trialLicenseOptions,
                        new LicensingClientListener());
            }
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        if (clusterService != null) {
            clusterService.remove(this);
        }
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
