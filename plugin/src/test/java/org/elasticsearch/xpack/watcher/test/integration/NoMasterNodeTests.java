/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.UnicastZenPing;
import org.elasticsearch.discovery.zen.ZenPing;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.watcher.WatcherService;
import org.elasticsearch.xpack.watcher.WatcherState;
import org.elasticsearch.xpack.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.watcher.client.WatchSourceBuilders;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.delete.DeleteWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.stats.WatcherStatsResponse;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

//test is just too slow, please fix it to not be sleep-based
@BadApple(bugUrl = "https://github.com/elastic/x-plugins/issues/1007")
@TestLogging("org.elasticsearch.discovery:TRACE,org.elasticsearch.watcher:TRACE")
@ClusterScope(scope = TEST, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, numDataNodes = 0,
        autoMinMasterNodes = false)
public class NoMasterNodeTests extends AbstractWatcherIntegrationTestCase {
    private ClusterDiscoveryConfiguration.UnicastZen config;

    @Override
    protected boolean timeWarped() {
        return false;
    }

    @Override
    protected boolean enableSecurity() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
        Settings unicastSettings = config.nodeSettings(nodeOrdinal);
        return Settings.builder()
                .put(settings)
                .put(unicastSettings)
                .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 2)
                .put(TestZenDiscovery.USE_MOCK_PINGS.getKey(), false)
                .build();
    }

    public void testSimpleFailure() throws Exception {
        // we need 3 hosts here because we stop the master and start another - it doesn't restart the pre-existing node...
        config = new ClusterDiscoveryConfiguration.UnicastZen(3, Settings.EMPTY);
        internalCluster().startNodes(2);
        createIndex("my-index");
        ensureWatcherStarted(false);

        // Have a sample document in the index, the watch is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        WatcherSearchTemplateRequest request = templateRequest(searchSource().query(termQuery("field", "value")), "my-index");
        WatchSourceBuilder watchSource = watchBuilder()
                .trigger(schedule(cron("0/5 * * * * ? *")))
                .input(searchInput(request))
                .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L));

        // we first need to make sure the license is enabled, otherwise all APIs will be blocked
        ensureLicenseEnabled();

        watcherClient().preparePutWatch("my-first-watch")
                .setSource(watchSource)
                .get();
        assertWatchWithMinimumPerformedActionsCount("my-first-watch", 1);

        // Stop the elected master, no new master will be elected b/c of m_m_n is set to 2
        stopElectedMasterNodeAndWait();
        try {
            // any watch action should fail, because there is no elected master node
            watcherClient().prepareDeleteWatch("my-first-watch").setMasterNodeTimeout(TimeValue.timeValueSeconds(1)).get();
            fail();
        } catch (Exception e) {
            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(MasterNotDiscoveredException.class));
        }
        // Bring back the 2nd node and wait for elected master node to come back and watcher to work as expected.
        startElectedMasterNodeAndWait();

        // we first need to make sure the license is enabled, otherwise all APIs will be blocked
        ensureLicenseEnabled();

        // Our first watch's condition should at least have been met twice
        assertWatchWithMinimumPerformedActionsCount("my-first-watch", 2);

        // Delete the existing watch
        DeleteWatchResponse response = watcherClient().prepareDeleteWatch("my-first-watch").get();
        assertThat(response.isFound(), is(true));

        // Add a new watch and wait for its condition to be met
        watcherClient().preparePutWatch("my-second-watch")
                .setSource(watchSource)
                .get();
        assertWatchWithMinimumPerformedActionsCount("my-second-watch", 1);
    }

    public void testDedicatedMasterNodeLayout() throws Exception {
        // Only the master nodes are in the unicast nodes list:
        config = new ClusterDiscoveryConfiguration.UnicastZen(11, 3, Settings.EMPTY);
        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNodes(7);
        ensureWatcherStarted(false);
        ensureLicenseEnabled();

        WatchSourceBuilder watchSource = WatchSourceBuilders.watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(simpleInput("key", "value"))
                .condition(AlwaysCondition.INSTANCE)
                .addAction("_id", loggingAction("executed!"));

        watcherClient().preparePutWatch("_watch_id")
                .setSource(watchSource)
                .get();
        assertWatchWithMinimumPerformedActionsCount("_watch_id", 1, false);

        // We still have 2 master node, we should recover from this failure:
        internalCluster().stopCurrentMasterNode();
        ensureWatcherStarted(false);
        ensureWatcherOnlyRunningOnce();
        assertWatchWithMinimumPerformedActionsCount("_watch_id", 2, false);

        // Stop the elected master, no new master will be elected b/c of m_m_n is set to 2
        stopElectedMasterNodeAndWait();
        try {
            // any watch action should fail, because there is no elected master node
            watcherClient().prepareDeleteWatch("_watch_id").setMasterNodeTimeout(TimeValue.timeValueSeconds(1)).get();
            fail();
        } catch (Exception e) {
            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(MasterNotDiscoveredException.class));
        }
        // Bring back the 2nd node and wait for elected master node to come back and watcher to work as expected.
        startElectedMasterNodeAndWait();

        // we first need to make sure the license is enabled, otherwise all APIs will be blocked
        ensureLicenseEnabled();

        // Our first watch's condition should at least have been met twice
        assertWatchWithMinimumPerformedActionsCount("_watch_id", 3, false);
    }

    public void testMultipleFailures() throws Exception {
        int numberOfFailures = scaledRandomIntBetween(2, 9);
        int numberOfWatches = scaledRandomIntBetween(numberOfFailures, 12);
        logger.info("number of failures [{}], number of watches [{}]", numberOfFailures, numberOfWatches);
        config = new ClusterDiscoveryConfiguration.UnicastZen(2 + numberOfFailures, Settings.EMPTY);
        internalCluster().startNodes(2);
        createIndex("my-index");
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();

        // watcher starts in the background, it can happen we get here too soon, so wait until watcher has started.
        ensureWatcherStarted(false);
        ensureLicenseEnabled();
        for (int i = 1; i <= numberOfWatches; i++) {
            String watchName = "watch" + i;
            WatcherSearchTemplateRequest request = templateRequest(searchSource().query(termQuery("field", "value")), "my-index");
            WatchSourceBuilder watchSource = watchBuilder()
                    .trigger(schedule(cron("0/5 * * * * ? *")))
                    .input(searchInput(request))
                    .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L));
            watcherClient().preparePutWatch(watchName).setSource(watchSource).get();
        }
        ensureGreen();

        for (int i = 1; i <= numberOfFailures; i++) {
            logger.info("failure round {}", i);

            for (int j = 1; j < numberOfWatches; j++) {
                String watchName = "watch" + i;
                assertWatchWithMinimumPerformedActionsCount(watchName, i);
            }
            ensureGreen();
            stopElectedMasterNodeAndWait();
            startElectedMasterNodeAndWait();

            WatcherStatsResponse statsResponse = watcherClient().prepareWatcherStats().get();
            assertThat(statsResponse.getWatchesCount(), equalTo((long) numberOfWatches));
        }
    }

    private void stopElectedMasterNodeAndWait() throws Exception {
        // Due to the fact that the elected master node gets stopped before the ping timeout has passed,
        // the unicast ping may have cached ping responses. This can cause the existing node to think
        // that there is another node and if its node id is lower than the node that has been stopped it
        // will elect itself as master. This is bad and should be fixed in core. What I think that should happen is that
        // if a node detects that is has lost a node, a node should clear its unicast temporal responses or at least
        // remove the node that has been removed. This is a workaround:
        ZenPing zenPing = ((TestZenDiscovery)internalCluster().getInstance(Discovery.class)).getZenPing();
        if (zenPing instanceof UnicastZenPing) {
            ((UnicastZenPing) zenPing).clearTemporalResponses();
        }
        internalCluster().stopCurrentMasterNode();
        // Can't use ensureWatcherStopped, b/c that relies on the watcher stats api which requires an elected master node
        assertBusy(new Runnable() {
            @Override
            public void run () {
                for (Client client : clients()) {
                    ClusterState state = client.admin().cluster().prepareState().setLocal(true).get().getState();
                    assertThat("Node [" + state.nodes().getLocalNode() + "] should have a NO_MASTER_BLOCK",
                            state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), is(true));
                }
            }
        }, 30, TimeUnit.SECONDS);
        // Ensure that the watch manager doesn't run elsewhere
        for (WatcherService watcherService : internalCluster().getInstances(WatcherService.class)) {
            assertThat(watcherService.state(), is(WatcherState.STOPPED));
        }
        for (ExecutionService executionService : internalCluster().getInstances(ExecutionService.class)) {
            assertThat(executionService.executionThreadPoolQueueSize(), equalTo(0L));
        }
    }

    private void startElectedMasterNodeAndWait() throws Exception {
        internalCluster().startNode();
        ensureWatcherStarted(false);
        ensureWatcherOnlyRunningOnce();
    }

}
