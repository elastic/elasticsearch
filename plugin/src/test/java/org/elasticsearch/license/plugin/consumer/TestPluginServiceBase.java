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
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.license.plugin.core.LicensesService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class TestPluginServiceBase extends AbstractLifecycleComponent<TestPluginServiceBase> implements ClusterStateListener {

    private LicensesClientService licensesClientService;

    private final ClusterService clusterService;
    // specify the trial license spec for the feature
    // example: 30 day trial on 1000 nodes
    final LicensesService.TrialLicenseOptions trialLicenseOptions;

    final boolean eagerLicenseRegistration;

    final static Collection<LicensesService.ExpirationCallback> expirationCallbacks;

    static {
        // Callback triggered every 24 hours from 30 days  to 7 days of license expiry
        final LicensesService.ExpirationCallback.Pre LEVEL_1 = new LicensesService.ExpirationCallback.Pre(TimeValue.timeValueHours(7 * 24), TimeValue.timeValueHours(30 * 24), TimeValue.timeValueHours(24)) {
            @Override
            public void on(License license, LicensesService.ExpirationStatus status) {
            }
        };

        // Callback triggered every 10 minutes from 7 days to license expiry
        final LicensesService.ExpirationCallback.Pre LEVEL_2 = new LicensesService.ExpirationCallback.Pre(null, TimeValue.timeValueHours(7 * 24), TimeValue.timeValueMinutes(10)) {
            @Override
            public void on(License license, LicensesService.ExpirationStatus status) {
            }
        };

        // Callback triggered every 10 minutes after license expiry
        final LicensesService.ExpirationCallback.Post LEVEL_3 = new LicensesService.ExpirationCallback.Post(TimeValue.timeValueMillis(0), null, TimeValue.timeValueMinutes(10)) {
            @Override
            public void on(License license, LicensesService.ExpirationStatus status) {
            }
        };

        expirationCallbacks = new ArrayList<>();
        expirationCallbacks.add(LEVEL_1);
        expirationCallbacks.add(LEVEL_2);
        expirationCallbacks.add(LEVEL_3);
    }


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
                        trialLicenseOptions, expirationCallbacks,
                        new LicensingClientListener());
            }
        }
    }

    protected void doStart() throws ElasticsearchException {
        if (eagerLicenseRegistration) {
            if (registered.compareAndSet(false, true)) {
                logger.info("Registering to licensesService [eager]");
                licensesClientService.register(featureName(),
                        trialLicenseOptions, expirationCallbacks,
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
        public void onEnabled(License license) {
            logger.info("TestConsumerPlugin: " + license.feature() + " enabled");
            enabled.set(true);
        }

        @Override
        public void onDisabled(License license) {
            if (license != null) {
                logger.info("TestConsumerPlugin: " + license.feature() + " disabled");
            }
            enabled.set(false);
        }

    }
}
