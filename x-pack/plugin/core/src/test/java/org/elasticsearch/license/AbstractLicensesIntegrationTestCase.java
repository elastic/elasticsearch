/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractLicensesIntegrationTestCase extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, CommonAnalysisPlugin.class);
    }

    protected void putLicense(final License license) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        clusterService.submitStateUpdateTask("putting license", new ClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                mdBuilder.putCustom(LicensesMetaData.TYPE, new LicensesMetaData(license, null));
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, @Nullable Exception e) {
                logger.error("error on metaData cleanup after test", e);
            }
        });
        latch.await();
    }

    protected void putLicenseTombstone() throws InterruptedException {
        putLicense(LicensesMetaData.LICENSE_TOMBSTONE);
    }

    protected void wipeAllLicenses() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        clusterService.submitStateUpdateTask("delete licensing metadata", new ClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                mdBuilder.removeCustom(LicensesMetaData.TYPE);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, @Nullable Exception e) {
                logger.error("error on metaData cleanup after test", e);
            }
        });
        latch.await();
    }

    protected void assertLicenseActive(boolean active) throws InterruptedException {
        boolean success = awaitBusy(() -> {
            for (XPackLicenseState licenseState : internalCluster().getDataNodeInstances(XPackLicenseState.class)) {
                if (licenseState.isActive() == active) {
                    return true;
                }
            }
            return false;
        });
        assertTrue(success);
    }

}
