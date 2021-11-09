/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

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
import org.junit.After;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class ILMHistoryStoreIT extends ESIntegTestCase {

    @After
    public void cleanup() {
        DeleteDataStreamAction.Request deleteDataStreamsRequest = new DeleteDataStreamAction.Request("*");
        deleteDataStreamsRequest.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_CLOSED_HIDDEN);
        assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, deleteDataStreamsRequest).actionGet());
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

    public void testPutAsyncStressTest() throws Exception {
        final String master = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        IndexLifecycleService indexLifecycleService = internalCluster().getInstance(IndexLifecycleService.class, master);
        ILMHistoryStore ilmHistoryStore = indexLifecycleService.getIlmHistoryStore();
        assertNotNull(ilmHistoryStore);

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
        Thread.sleep(1000);

        ilmHistoryStore.close();

        assertFalse(true);
    }
}
