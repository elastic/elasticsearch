/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class GetProfilingActionIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ProfilingPlugin.class);
    }

    private byte[] read(String resource) throws IOException {
        return GetProfilingAction.class.getClassLoader().getResourceAsStream(resource).readAllBytes();
    }

    private void createIndex(String name, String bodyFileName) throws Exception {
        client()
            .admin()
            .indices()
            .prepareCreate(name)
            .setSource(read(bodyFileName), XContentType.JSON)
            .execute()
            .get();
    }

    private void indexDoc(String index, String id, Map<String, Object> source) {
        IndexResponse indexResponse = client().prepareIndex(index).setId(id).setSource(source).get();
        assertEquals(RestStatus.CREATED, indexResponse.status());
    }

    @Before
    public void setupData() throws Exception {

        for (String idx : EventsIndex.indexNames()) {
            createIndex(idx, "events.json");
        }
        createIndex("profiling-stackframes", "stackframes.json");
        createIndex("profiling-stacktraces", "stacktraces.json");
        createIndex("profiling-executables", "executables.json");
        ensureGreen();

        // ensure that we have this in every index, so we find an event
        for (String idx : EventsIndex.indexNames()) {
            indexDoc(idx, "QjoLteG7HX3VUUXr-J4kHQ",
                Map.of("@timestamp", 1668761065, "Stacktrace.id", "QjoLteG7HX3VUUXr-J4kHQ", "Stacktrace.count", 1));
        }

        indexDoc("profiling-stacktraces", "QjoLteG7HX3VUUXr-J4kHQ",
            Map.of("Stacktrace.frame.ids", "QCCDqjSg3bMK1C4YRK6TiwAAAAAAEIpf", "Stacktrace.frame.types", "AQI"));
        indexDoc("profiling-stackframes", "QCCDqjSg3bMK1C4YRK6TiwAAAAAAEIpf",
            Map.of("Stackframe.function.name", "_raw_spin_unlock_irqrestore"));
        indexDoc("profiling-executables", "QCCDqjSg3bMK1C4YRK6Tiw", Map.of("Executable.file.name", "libc.so.6"));

        refresh();
    }

    public void testGetProfilingDataUnfiltered() throws Exception {
        GetProfilingRequest request = new GetProfilingRequest(1, null);
        GetProfilingResponse response = client().execute(GetProfilingAction.INSTANCE, request).get();
        assertEquals(RestStatus.OK, response.status());
        assertEquals(1, response.getTotalFrames());
        assertNotNull(response.getStackTraces());
        StackTrace stackTrace = response.getStackTraces().get("QjoLteG7HX3VUUXr-J4kHQ");
        assertArrayEquals(new int[] {1083999}, stackTrace.addressOrLines);
        assertArrayEquals(new String[] {"QCCDqjSg3bMK1C4YRK6Tiw"}, stackTrace.fileIds);
        assertArrayEquals(new String[] {"QCCDqjSg3bMK1C4YRK6TiwAAAAAAEIpf"}, stackTrace.frameIds);
        assertArrayEquals(new int[] {2}, stackTrace.typeIds);

        assertNotNull(response.getStackFrames());
        StackFrame stackFrame = response.getStackFrames().get("QCCDqjSg3bMK1C4YRK6TiwAAAAAAEIpf");
        assertEquals("_raw_spin_unlock_irqrestore", stackFrame.functionName);
        assertNotNull(response.getStackTraceEvents());
        assertEquals(1, (int) response.getStackTraceEvents().get("QjoLteG7HX3VUUXr-J4kHQ"));

        assertNotNull(response.getExecutables());
        assertNotNull("libc.so.6", response.getExecutables().get("QCCDqjSg3bMK1C4YRK6Tiw"));
    }
}
