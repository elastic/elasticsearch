/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.hasSize;

public class DataNodeRequestSenderIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testRetryOnShardMovement() {
        internalCluster().ensureAtLeastNumDataNodes(2);

        var index = randomIdentifier();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
        );
        client().prepareBulk(index)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(new IndexRequest().source("key", "value"))
            .get();

        var shouldMove = new AtomicBoolean(true);

        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            as(transportService, MockTransportService.class).addRequestHandlingBehavior(
                ExchangeService.OPEN_EXCHANGE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    // move index shard
                    if (shouldMove.compareAndSet(true, false)) {
                        var currentShardNodeId = clusterService().state().routingTable().index(index).shard(0).primaryShard().currentNodeId();
                        assertAcked(
                            client().admin()
                                .indices()
                                .prepareUpdateSettings(index)
                                .setSettings(Settings.builder().put("index.routing.allocation.exclude._id", currentShardNodeId))
                        );
                        ensureGreen(index);
                    }
                    // execute data node request
                    handler.messageReceived(request, channel, task);
                }
            );
        }

        try (EsqlQueryResponse resp = run("FROM " + index + "*")) {
            assertThat(getValuesList(resp), hasSize(1));
        }
    }
}
