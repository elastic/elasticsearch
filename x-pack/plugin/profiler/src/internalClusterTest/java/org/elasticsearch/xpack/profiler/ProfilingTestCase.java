/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public abstract class ProfilingTestCase extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            LocalStateCompositeXPackPlugin.class,
            DataStreamsPlugin.class,
            ProfilingPlugin.class,
            IndexLifecycle.class,
            UnsignedLongMapperPlugin.class,
            getTestTransportPlugin()
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
            .put(XPackSettings.PROFILING_ENABLED.getKey(), true)
            .put(ProfilingPlugin.PROFILING_TEMPLATES_ENABLED.getKey(), false)
            // .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            // Disable ILM history index so that the tests don't have to clean it up
            .put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.getKey(), false)
            .build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    private void indexDoc(String index, String id, Map<String, Object> source) {
        IndexResponse indexResponse = client().prepareIndex(index).setId(id).setSource(source).setCreate(true).get();
        assertEquals(RestStatus.CREATED, indexResponse.status());
    }

    /**
     * Only the index "profiling-events-all" is always present. All other indices (e.g. "profiling-events-5pow02") are created on demand
     * at a later point when there are enough samples. With this flag we simulate that data should be retrieved briefly after cluster
     * start when only profiling-events-all is present. We expect that also in this case, available data is returned but we rely only
     * on the single existing index.
     *
     * @return <code>true</code> iff this test should rely on only "profiling-events-all" being present.
     */
    protected abstract boolean useOnlyAllEvents();

    protected void waitForIndices() throws Exception {
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            assertTrue(
                "Timed out waiting for the indices to be created",
                state.metadata().indices().keySet().containsAll(ProfilingIndexManager.INDICES_AND_ALIASES.keySet())
            );
        });
    }

    protected void updateProfilingTemplatesEnabled(boolean newValue) {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(Settings.builder().put(ProfilingPlugin.PROFILING_TEMPLATES_ENABLED.getKey(), newValue).build());
        ClusterUpdateSettingsResponse response = client().admin().cluster().updateSettings(request).actionGet();
        assertTrue("Update of profiling templates enabled setting is not acknowledged", response.isAcknowledged());
    }

    @Before
    public void setupData() throws Exception {
        // only enable index management while setting up indices to avoid interfering with the rest of the test infrastructure
        updateProfilingTemplatesEnabled(true);
        Collection<String> eventsIndices = useOnlyAllEvents() ? List.of(EventsIndex.FULL_INDEX.getName()) : EventsIndex.indexNames();
        waitForIndices();
        ensureGreen();

        // ensure that we have this in every index, so we find an event
        for (String idx : eventsIndices) {
            indexDoc(
                idx,
                "QjoLteG7HX3VUUXr-J4kHQ",
                Map.of("@timestamp", 1668761065, "Stacktrace.id", "QjoLteG7HX3VUUXr-J4kHQ", "Stacktrace.count", 1)
            );
        }

        indexDoc(
            "profiling-stacktraces",
            "QjoLteG7HX3VUUXr-J4kHQ",
            Map.of("Stacktrace.frame.ids", "QCCDqjSg3bMK1C4YRK6TiwAAAAAAEIpf", "Stacktrace.frame.types", "AQI")
        );
        indexDoc(
            "profiling-stackframes",
            "QCCDqjSg3bMK1C4YRK6TiwAAAAAAEIpf",
            Map.of("Stackframe.function.name", List.of("_raw_spin_unlock_irqrestore", "inlined_frame_1", "inlined_frame_0"))
        );
        indexDoc("profiling-executables", "QCCDqjSg3bMK1C4YRK6Tiw", Map.of("Executable.file.name", "libc.so.6"));

        refresh();
    }

    @After
    public void disable() {
        updateProfilingTemplatesEnabled(false);
    }
}
