/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED;
import static org.elasticsearch.xpack.ilm.history.ILMHistoryStore.ILM_HISTORY_DATA_STREAM;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ILMHistoryTests extends ESIntegTestCase {

    public static final String[] DATA_STREAM_NAMES = {ILM_HISTORY_DATA_STREAM};

    @After
    public void cleanUp() {
        client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(DATA_STREAM_NAMES)).actionGet();
    }

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
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class, DataStreamsPlugin.class);
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

        assertBusy(() -> {
            GetDataStreamAction.Request request = new GetDataStreamAction.Request(DATA_STREAM_NAMES);
            try {
                GetDataStreamAction.Response response = client().execute(GetDataStreamAction.INSTANCE, request).actionGet();
                assertThat(response.getDataStreams(), hasSize(1));
            } catch (IndexNotFoundException e) {
                fail(e.getMessage());
            }
        });

        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Map.of(LIFECYCLE_HISTORY_INDEX_ENABLED, false)).get();

        RolloverResponse rolloverResponse = client().admin().indices().prepareRolloverIndex(ILM_HISTORY_DATA_STREAM).get();

        assertTrue(rolloverResponse.isAcknowledged());
        String newIndex = DataStream.getDefaultBackingIndexName(ILM_HISTORY_DATA_STREAM, 2);
        assertThat(rolloverResponse.getNewIndex(), is(newIndex));

        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(newIndex).get();
        assertThat(getIndexResponse.getIndices(), arrayContaining(newIndex));
    }


}
