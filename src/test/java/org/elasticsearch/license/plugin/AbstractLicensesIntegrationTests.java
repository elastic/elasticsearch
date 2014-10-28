/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicensesMetaData;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.util.concurrent.CountDownLatch;

/**
 */
public abstract class AbstractLicensesIntegrationTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("plugins.load_classpath_plugins", false)
                .put("plugin.types", LicensePlugin.class.getName())
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        // Plugin should be loaded on the transport client as well
        return nodeSettings(0);
    }

    protected void wipeAllLicenses() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        clusterService.submitStateUpdateTask("delete licensing metadata", new ProcessedClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                mdBuilder.putCustom(LicensesMetaData.TYPE, null);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, @Nullable Throwable t) {
                logger.error("error on metaData cleanup after test", t);
            }
        });
        latch.await();
    }

}
