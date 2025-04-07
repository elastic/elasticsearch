/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.ComputeService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.shard.IndexShardTestCase.closeShardNoCheck;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class EsqlRetryIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    public void testRetryOnShardFailures() throws Exception {
        populateIndices();
        try {
            final AtomicBoolean relocated = new AtomicBoolean();
            for (String node : internalCluster().getNodeNames()) {
                // fail some target shards while handling the data node request
                MockTransportService.getInstance(node)
                    .addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                        if (relocated.compareAndSet(false, true)) {
                            closeOrFailShards(node);
                        }
                        handler.messageReceived(request, channel, task);
                    });
            }
            try (var resp = run("FROM log-* | STATS COUNT(timestamp) | LIMIT 1")) {
                assertThat(EsqlTestUtils.getValuesList(resp).get(0).get(0), equalTo(7L));
            }
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
        }
    }

    private void populateIndices() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(prepareCreate("log-index-1").setSettings(indexSettings(between(1, 3), 1)).setMapping("timestamp", "type=date"));
        assertAcked(prepareCreate("log-index-2").setSettings(indexSettings(between(1, 3), 1)).setMapping("timestamp", "type=date"));
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(prepareIndex("log-index-1").setSource("timestamp", "2015-07-08"));
        reqs.add(prepareIndex("log-index-1").setSource("timestamp", "2018-07-08"));
        reqs.add(prepareIndex("log-index-1").setSource("timestamp", "2020-03-03"));
        reqs.add(prepareIndex("log-index-1").setSource("timestamp", "2020-09-09"));
        reqs.add(prepareIndex("log-index-2").setSource("timestamp", "2019-10-12"));
        reqs.add(prepareIndex("log-index-2").setSource("timestamp", "2020-02-02"));
        reqs.add(prepareIndex("log-index-2").setSource("timestamp", "2020-10-10"));
        indexRandom(true, reqs);
        ensureGreen("log-index-1", "log-index-2");
        indicesAdmin().prepareRefresh("log-index-1", "log-index-2").get();
    }

    private void closeOrFailShards(String nodeName) throws Exception {
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                if (randomBoolean()) {
                    indexShard.failShard("simulated", new IOException("simulated failure"));
                } else if (randomBoolean()) {
                    closeShardNoCheck(indexShard);
                }
            }
        }
    }
}
