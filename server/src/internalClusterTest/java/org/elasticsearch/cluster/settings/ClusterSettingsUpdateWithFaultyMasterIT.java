/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.settings;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ClusterSettingsUpdateWithFaultyMasterIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(BlockingClusterSettingTestPlugin.class, MockTransportService.TestPlugin.class);
    }

    public void testClusterSettingsUpdateNotAcknowledged() throws Exception {
        final var nodes = internalCluster().startMasterOnlyNodes(3);
        final String masterNode = internalCluster().getMasterName();
        final String blockedNode = randomValueOtherThan(masterNode, () -> randomFrom(nodes));
        assertThat(blockedNode, not(equalTo(internalCluster().getMasterName())));
        ensureStableCluster(3);

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(
                Set.of(blockedNode),
                nodes.stream().filter(n -> n.equals(blockedNode) == false).collect(Collectors.toSet())
            ),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        logger.debug("--> updating cluster settings");
        var future = client(masterNode).admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(BlockingClusterSettingTestPlugin.TEST_BLOCKING_SETTING.getKey(), true).build())
            .setMasterNodeTimeout(TimeValue.timeValueMillis(10L))
            .execute();

        logger.debug("--> waiting for cluster state update to be blocked");
        BlockingClusterSettingTestPlugin.blockLatch.await();

        logger.debug("--> isolating master eligible node [{}] from other nodes", blockedNode);
        networkDisruption.startDisrupting();

        logger.debug("--> unblocking cluster state update");
        BlockingClusterSettingTestPlugin.releaseLatch.countDown();

        assertThat("--> cluster settings update should not be acknowledged", future.get().isAcknowledged(), equalTo(false));

        logger.debug("--> stop network disruption");
        networkDisruption.stopDisrupting();
        ensureStableCluster(3);
    }

    public static class BlockingClusterSettingTestPlugin extends Plugin {

        private static final Logger logger = LogManager.getLogger(BlockingClusterSettingTestPlugin.class);

        private static final CountDownLatch blockLatch = new CountDownLatch(1);
        private static final CountDownLatch releaseLatch = new CountDownLatch(1);
        private static final AtomicBoolean blockOnce = new AtomicBoolean();

        public static final Setting<Boolean> TEST_BLOCKING_SETTING = Setting.boolSetting("cluster.test.blocking_setting", false, value -> {
            if (blockOnce.compareAndSet(false, true)) {
                logger.debug("--> setting validation is now blocking cluster state update");
                blockLatch.countDown();
                try {
                    logger.debug("--> setting validation is now waiting for release");
                    releaseLatch.await();
                    logger.debug("--> setting validation is done");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
        }, Setting.Property.NodeScope, Setting.Property.Dynamic);

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(TEST_BLOCKING_SETTING);
        }
    }
}
