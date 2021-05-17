/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class BulkRejectionIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("thread_pool.write.size", 1)
            .put("thread_pool.write.queue_size", 1)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class);
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder().put(super.indexSettings())
            // sync global checkpoint quickly so we can verify seq_no_stats aligned between all copies after tests.
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s").build();
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    protected int numberOfShards() {
        return 5;
    }

    public void testBulkRejectionAfterDynamicMappingUpdate() throws Exception {
        final String index = "test";
        assertAcked(prepareCreate(index));
        ensureGreen();
        final BulkRequest request1 = new BulkRequest();
        for (int i = 0; i < 500; ++i) {
            request1.add(new IndexRequest(index).source(Collections.singletonMap("key" + i, "value" + i)))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
        // Huge request to keep the write pool busy so that requests waiting on a mapping update in the other bulk request get rejected
        // by the write pool
        final BulkRequest request2 = new BulkRequest();
        for (int i = 0; i < 10_000; ++i) {
            request2.add(new IndexRequest(index).source(Collections.singletonMap("key", "valuea" + i)))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
        final ActionFuture<BulkResponse> bulkFuture1 = client().bulk(request1);
        final ActionFuture<BulkResponse> bulkFuture2 = client().bulk(request2);
        try {
            bulkFuture1.actionGet();
            bulkFuture2.actionGet();
        } catch (EsRejectedExecutionException e) {
            // ignored, one of the two bulk requests was rejected outright due to the write queue being full
        }
        internalCluster().assertSeqNos();
    }
}
