/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.LeaderChecker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportSettings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;

@ESIntegTestCase.ClusterScope(scope = TEST, minNumDataNodes = 2, maxNumDataNodes = 4)
public class EsqlDisruptionIT extends EsqlActionIT {

    // copied from AbstractDisruptionTestCase
    public static final Settings DEFAULT_SETTINGS = Settings.builder()
        .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "5s") // for hitting simulated network failures quickly
        .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), 1) // for hitting simulated network failures quickly
        .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "5s") // for hitting simulated network failures quickly
        .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1) // for hitting simulated network failures quickly
        .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "5s") // <-- for hitting simulated network failures quickly
        .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "10s") // Network delay disruption waits for the min between this
        // value and the time of disruption and does not recover immediately
        // when disruption is stop. We should make sure we recover faster
        // than the default of 30s, causing ensureGreen and friends to time out
        .build();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(DEFAULT_SETTINGS)
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(between(3000, 4000)))
            .build();
        logger.info("settings {}", settings);
        return settings;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        return plugins;
    }

    @Override
    protected EsqlQueryResponse run(EsqlQueryRequest request) {
        // IndexResolver currently ignores failures from field-caps responses and can resolve to a smaller set of concrete indices.
        boolean singleIndex = request.query().startsWith("from test |");
        if (singleIndex && randomIntBetween(0, 100) <= 20) {
            return runQueryWithDisruption(request);
        } else {
            return super.run(request);
        }
    }

    private EsqlQueryResponse runQueryWithDisruption(EsqlQueryRequest request) {
        final ServiceDisruptionScheme disruptionScheme = addRandomDisruptionScheme();
        logger.info("--> start disruption scheme [{}]", disruptionScheme);
        disruptionScheme.startDisrupting();
        logger.info("--> executing esql query with disruption {} ", request.query());
        ActionFuture<EsqlQueryResponse> future = client().execute(EsqlQueryAction.INSTANCE, request);
        try {
            return future.actionGet(2, TimeUnit.MINUTES);
        } catch (Exception ignored) {

        } finally {
            clearDisruption();
        }
        try {
            return future.actionGet(2, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.info(
                "running tasks: {}",
                client().admin()
                    .cluster()
                    .prepareListTasks()
                    .get()
                    .getTasks()
                    .stream()
                    .filter(
                        // Skip the tasks we that'd get in the way while debugging
                        t -> false == t.action().contains(TransportListTasksAction.TYPE.name())
                            && false == t.action().contains(HealthNode.TASK_NAME)
                    )
                    .toList()
            );
            assertTrue("request must be failed or completed after clearing disruption", future.isDone());
            ensureBlocksReleased();
            logger.info("--> failed to execute esql query with disruption; retrying...", e);
            return client().execute(EsqlQueryAction.INSTANCE, request).actionGet(2, TimeUnit.MINUTES);
        }
    }

    private ServiceDisruptionScheme addRandomDisruptionScheme() {
        try {
            ensureClusterStateConsistency();
            ensureClusterSizeConsistency();
            var disruptedLinks = NetworkDisruption.TwoPartitions.random(random(), internalCluster().getNodeNames());
            final NetworkDisruption.NetworkLinkDisruptionType disruptionType = switch (randomInt(2)) {
                case 0 -> NetworkDisruption.UNRESPONSIVE;
                case 1 -> NetworkDisruption.DISCONNECT;
                case 2 -> NetworkDisruption.NetworkDelay.random(random(), TimeValue.timeValueMillis(2000), TimeValue.timeValueMillis(5000));
                default -> throw new IllegalArgumentException();
            };
            final ServiceDisruptionScheme scheme = new NetworkDisruption(disruptedLinks, disruptionType);
            setDisruptionScheme(scheme);
            return scheme;
        } catch (Exception e) {
            throw new AssertionError(e);
        }

    }

    private void clearDisruption() {
        logger.info("--> clear disruption scheme");
        try {
            internalCluster().clearDisruptionScheme(false);
            ensureFullyConnectedCluster();
            assertBusy(() -> ClusterRerouteUtils.rerouteRetryFailed(client()), 1, TimeUnit.MINUTES);
            ensureYellow();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
