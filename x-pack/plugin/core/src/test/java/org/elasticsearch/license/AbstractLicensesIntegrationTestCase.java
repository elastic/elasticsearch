/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractLicensesIntegrationTestCase extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, CommonAnalysisPlugin.class);
    }

    protected void putLicense(final License license) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        clusterService.submitUnbatchedStateUpdateTask("putting license", new ClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                mdBuilder.putCustom(LicensesMetadata.TYPE, new LicensesMetadata(license, null));
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(@Nullable Exception e) {
                logger.error("error on metadata cleanup after test", e);
            }
        });
        latch.await();
    }

    protected void putLicenseTombstone() throws InterruptedException {
        putLicense(LicensesMetadata.LICENSE_TOMBSTONE);
    }

    protected void wipeAllLicenses() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        clusterService.submitUnbatchedStateUpdateTask("delete licensing metadata", new ClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                mdBuilder.removeCustom(LicensesMetadata.TYPE);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(@Nullable Exception e) {
                logger.error("error on metadata cleanup after test", e);
            }
        });
        latch.await();
    }

    protected void assertLicenseActive(boolean active) throws Exception {
        assertBusy(() -> {
            for (XPackLicenseState licenseState : internalCluster().getDataNodeInstances(XPackLicenseState.class)) {
                if (licenseState.isActive() == active) {
                    return;
                }
            }
            fail("No data nodes have a license active state of [" + active + "]");
        });
    }

}
