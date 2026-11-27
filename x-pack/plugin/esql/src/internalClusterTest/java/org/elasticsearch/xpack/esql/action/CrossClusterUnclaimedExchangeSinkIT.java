/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.ComputeService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Tests that an exchange sink that no cluster request will claim is removed from the remote cluster at once, not by its
 * inactive-sink reaper.
 */
public class CrossClusterUnclaimedExchangeSinkIT extends AbstractCrossClusterTestCase {

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, EsqlUnclaimedExchangeSinkIT.INACTIVE_SINKS_INTERVAL)
            .build();
    }

    /**
     * A query cancelled between the open on the remote cluster and the creation of the compute group task of that cluster leaves the
     * remote cluster with a sink that no cluster request will claim. The querying cluster removes it.
     */
    public void testCancelBeforeGroupTaskRemovesOpenedSink() throws Exception {
        populateIndex(REMOTE_CLUSTER_1, REMOTE_INDEX, 1, 10);
        InternalTestCluster local = cluster(LOCAL_CLUSTER);
        InternalTestCluster remote = cluster(REMOTE_CLUSTER_1);
        String coordinator = randomFrom(local.getNodeNames());
        // the open goes to whichever node of the remote cluster the querying cluster is connected to
        Map<MockTransportService, ExchangeService> targets = new HashMap<>();
        for (String node : remote.getNodeNames()) {
            targets.put(
                (MockTransportService) remote.getInstance(TransportService.class, node),
                remote.getInstance(ExchangeService.class, node)
            );
        }
        EsqlUnclaimedExchangeSinkIT.assertCancelledQueryRemovesSink(
            EsqlUnclaimedExchangeSinkIT.CancelPoint.BEFORE_GROUP_TASK,
            local.client(coordinator),
            (MockTransportService) local.getInstance(TransportService.class, coordinator),
            targets,
            ComputeService.CLUSTER_ACTION_NAME,
            syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":" + REMOTE_INDEX + " | STATS COUNT(*)")
        );
    }
}
