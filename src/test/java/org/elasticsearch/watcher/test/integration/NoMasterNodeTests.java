/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.watcher.WatcherService;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.client.WatchSourceBuilders;
import org.elasticsearch.watcher.execution.ExecutionService;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.transport.actions.delete.DeleteWatchResponse;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsResponse;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

/**
 */
@Slow
@ClusterScope(scope = TEST, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, numDataNodes = 0)
public class NoMasterNodeTests extends AbstractWatcherIntegrationTests {

    private ClusterDiscoveryConfiguration.UnicastZen config;

    @Override
    protected boolean timeWarped() {
        return false;
    }

    @Override
    protected boolean enableShield() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
        Settings unicastSettings = config.node(nodeOrdinal);
        return ImmutableSettings.builder()
                .put(settings)
                .put(unicastSettings)
                .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, 2)
                .put("discovery.type", "zen")
                .build();
    }

    @Test
    public void testSimpleFailure() throws Exception {
        config = new ClusterDiscoveryConfiguration.UnicastZen(2);
        internalTestCluster().startNodesAsync(2).get();
        createIndex("my-index");
        ensureWatcherStarted(false);

        // Have a sample document in the index, the watch is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        SearchRequest searchRequest = WatcherTestUtils.newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference watchSource = createWatchSource("0/5 * * * * ? *", searchRequest, "ctx.payload.hits.total == 1");

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

    @Test
    public void testDedicatedMasterNodeLayout() throws Exception {
        // Only the master nodes are in the unicast nodes list:
        config = new ClusterDiscoveryConfiguration.UnicastZen(3);
        Settings settings = ImmutableSettings.builder().put("node.type", "master").build();
        internalTestCluster().startNodesAsync(3, settings).get();
        settings = ImmutableSettings.builder().put("node.type", "data").build();
        internalTestCluster().startNodesAsync(7, settings).get();
        ensureWatcherStarted(false);
        ensureLicenseEnabled();

        WatchSourceBuilder watchSource = WatchSourceBuilders.watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(simpleInput("key", "value"))
                .condition(alwaysCondition())
                .addAction("_id", loggingAction("[{{ctx.watch_id}}] executed!"));

        watcherClient().preparePutWatch("_watch_id")
                .setSource(watchSource)
                .get();
        assertWatchWithMinimumPerformedActionsCount("_watch_id", 1, false);

        // We still have 2 master node, we should recover from this failure:
        internalTestCluster().stopCurrentMasterNode();
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

    @Test
    @TestLogging("watcher:TRACE,cluster.service:TRACE,indices.recovery:TRACE,indices.cluster:TRACE")
    public void testMultipleFailures() throws Exception {
        int numberOfFailures = scaledRandomIntBetween(2, 9);
        int numberOfWatches = scaledRandomIntBetween(numberOfFailures, 12);
        logger.info("number of failures [{}], number of watches [{}]", numberOfFailures, numberOfWatches);
        config = new ClusterDiscoveryConfiguration.UnicastZen(2 + numberOfFailures);
        internalTestCluster().startNodesAsync(2).get();
        createIndex("my-index");
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();

        // watcher starts in the background, it can happen we get here too soon, so wait until watcher has started.
        ensureWatcherStarted(false);
        ensureLicenseEnabled();
        for (int i = 1; i <= numberOfWatches; i++) {
            String watchName = "watch" + i;
            SearchRequest searchRequest = WatcherTestUtils.newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
            BytesReference watchSource = createWatchSource("0/5 * * * * ? *", searchRequest, "ctx.payload.hits.total == 1");
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
        internalTestCluster().stopCurrentMasterNode();
        // Can't use ensureWatcherStopped, b/c that relies on the watcher stats api which requires an elected master node
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                for (Client client : clients()) {
                    ClusterState state = client.admin().cluster().prepareState().setLocal(true).get().getState();
                    if (!state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID)) {
                        return false;
                    }
                }
                return true;
            }
        }, 30, TimeUnit.SECONDS), equalTo(true));
        // Ensure that the watch manager doesn't run elsewhere
        for (WatcherService watcherService : internalTestCluster().getInstances(WatcherService.class)) {
            assertThat(watcherService.state(), is(WatcherService.State.STOPPED));
        }
        for (ExecutionService executionService : internalTestCluster().getInstances(ExecutionService.class)) {
            assertThat(executionService.queueSize(), equalTo(0l));
        }
    }

    private void startElectedMasterNodeAndWait() throws Exception {
        internalTestCluster().startNode();
        ensureWatcherStarted(false);
        ensureWatcherOnlyRunningOnce();
    }

}
