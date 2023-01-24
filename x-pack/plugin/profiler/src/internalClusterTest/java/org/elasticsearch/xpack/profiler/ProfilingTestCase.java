/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public abstract class ProfilingTestCase extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ProfilingPlugin.class, getTestTransportPlugin());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
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

    private byte[] read(String resource) throws IOException {
        return GetProfilingAction.class.getClassLoader().getResourceAsStream(resource).readAllBytes();
    }

    private void createIndex(String name, String bodyFileName) throws Exception {
        client().admin().indices().prepareCreate(name).setSource(read(bodyFileName), XContentType.JSON).execute().get();
    }

    private void indexDoc(String index, String id, Map<String, Object> source) {
        IndexResponse indexResponse = client().prepareIndex(index).setId(id).setSource(source).get();
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

    @Before
    public void setupData() throws Exception {
        Collection<String> eventsIndices = useOnlyAllEvents() ? List.of(EventsIndex.FULL_INDEX.getName()) : EventsIndex.indexNames();

        for (String idx : eventsIndices) {
            createIndex(idx, "events.json");
        }
        createIndex("profiling-stackframes", "stackframes.json");
        createIndex("profiling-stacktraces", "stacktraces.json");
        createIndex("profiling-executables", "executables.json");
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
}
