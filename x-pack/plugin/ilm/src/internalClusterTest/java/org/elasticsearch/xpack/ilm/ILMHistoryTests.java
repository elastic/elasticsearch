/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.StopILMRequest;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.StopILMAction;
import org.elasticsearch.xpack.ilm.history.ILMHistoryStore;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ILMHistoryTests extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");
        settings.put(LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false);
        return settings.build();
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class);
    }

    private void putTestPolicy() throws InterruptedException, java.util.concurrent.ExecutionException {
        Phase phase = new Phase("hot", TimeValue.ZERO, Collections.emptyMap());
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy("test", Collections.singletonMap("hot", phase));
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        PutLifecycleAction.Response putLifecycleResponse = client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get();
        assertAcked(putLifecycleResponse);
    }

    public void testIlmHistoryIndexCanRollover() throws Exception {
        putTestPolicy();
        Settings settings = Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0).put(LifecycleSettings.LIFECYCLE_NAME, "test").build();
        CreateIndexResponse res = client().admin().indices().prepareCreate("test").setSettings(settings).get();
        assertTrue(res.isAcknowledged());

        String firstIndex = ILMHistoryStore.ILM_HISTORY_INDEX_PREFIX + "000001";
        String secondIndex = ILMHistoryStore.ILM_HISTORY_INDEX_PREFIX + "000002";

        assertBusy(() -> {
            try {
                GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(firstIndex).get();
                assertThat(getIndexResponse.getIndices(), arrayContaining(firstIndex));
            } catch (Exception e) {
                fail(e.getMessage());
            }
        });

        //wait for all history items to index to avoid waiting for timeout in ILMHistoryStore beforeBulk
        assertBusy(() -> {
            try {
                SearchResponse search = client().prepareSearch(firstIndex).setQuery(matchQuery("index", firstIndex)).setSize(0).get();
                assertHitCount(search, 9);
            } catch (Exception e) {
                //assertBusy will stop on first non-assertion error and it can happen when we try to search too early
                //instead of failing the whole test change it to assertion error and wait some more time
                fail(e.getMessage());
            }
        }, 1L, TimeUnit.MINUTES);

        //make sure ILM is stopped so no new items will be queued in ILM history
        assertTrue(client().execute(StopILMAction.INSTANCE, new StopILMRequest()).actionGet().isAcknowledged());
        assertBusy(() -> {
            GetStatusAction.Response status = client().execute(GetStatusAction.INSTANCE, new GetStatusAction.Request()).actionGet();
            assertThat(status.getMode(), is(OperationMode.STOPPED));
        });

        RolloverResponse rolloverResponse = client().admin().indices().prepareRolloverIndex(ILMHistoryStore.ILM_HISTORY_ALIAS).get();

        assertTrue(rolloverResponse.isAcknowledged());
        assertThat(rolloverResponse.getNewIndex(), is(secondIndex));

        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(secondIndex).get();
        assertThat(getIndexResponse.getIndices(), arrayContaining(secondIndex));
    }
}
