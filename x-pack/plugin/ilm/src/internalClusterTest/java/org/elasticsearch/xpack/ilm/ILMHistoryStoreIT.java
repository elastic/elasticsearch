/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;
import org.elasticsearch.xpack.ilm.history.ILMHistoryItem;
import org.elasticsearch.xpack.ilm.history.ILMHistoryStore;
import org.hamcrest.Matchers;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.search.internal.SearchContext.TRACK_TOTAL_HITS_ACCURATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class ILMHistoryStoreIT extends ESIntegTestCase {

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    @After
    public void shutdownExec() {
        executorService.shutdownNow();
    }

    @After
    public void cleanup() {
        // the datastream delete uses the generic threadpool, so if that's locked up, then we'll hang at the actionGet -- hence the timeout
        DeleteDataStreamAction.Request deleteDataStreamsRequest = new DeleteDataStreamAction.Request("*");
        deleteDataStreamsRequest.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_CLOSED_HIDDEN);
        assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, deleteDataStreamsRequest).actionGet(5, TimeUnit.SECONDS));
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false);
        settings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");
        return settings.build();
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class, DataStreamsPlugin.class);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/68468")
    public void testPutAsyncStressTest() {
        final String master = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        IndexLifecycleService indexLifecycleService = internalCluster().getInstance(IndexLifecycleService.class, master);
        ILMHistoryStore ilmHistoryStore = indexLifecycleService.getIlmHistoryStore();
        assertNotNull(ilmHistoryStore);

        // in the background, putAsync a bunch of ILMHistoryItems, then close the ILMHistoryStore
        Future<?> future = executorService.submit(() -> {
            for (int i = 0; i < 8192; i++) {
                String index = randomAlphaOfLength(5);
                String policyId = randomAlphaOfLength(5);
                String phase = randomAlphaOfLength(5);
                final long timestamp = randomNonNegativeLong();
                ILMHistoryItem record = ILMHistoryItem.success(
                    index,
                    policyId,
                    timestamp,
                    10L,
                    LifecycleExecutionState.builder().setPhase(phase).build()
                );

                ilmHistoryStore.putAsync(record);
            }

            // since putAsync runs async, we need to give the other thread(s) a chance to run
            try {
                assertBusy(() -> {
                    SearchRequestBuilder searchRequestBuilder = client().prepareSearch(".ds-ilm-history*")
                        .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
                        .setSize(0)
                        .setTrackTotalHitsUpTo(TRACK_TOTAL_HITS_ACCURATE);
                    try {
                        SearchResponse countResponse = searchRequestBuilder.get();
                        TotalHits hits = countResponse.getHits().getTotalHits();
                        assertThat(hits.value, Matchers.greaterThanOrEqualTo(8192L));
                    } catch (SearchPhaseExecutionException e) {
                        fail(); // retry via the assertBusy
                    }
                }, 20, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // if the above doesn't deadlock, then we'll get a result eventually
        try {
            future.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail();
        }
    }
}
