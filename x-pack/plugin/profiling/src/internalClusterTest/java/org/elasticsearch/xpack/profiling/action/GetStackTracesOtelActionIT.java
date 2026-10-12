/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Tests the OTel read path with data indexed into {@code *.otel-*} data streams.
 * OTel profiling templates are installed by {@link OTelPlugin} at node startup.
 */
public class GetStackTracesOtelActionIT extends ProfilingTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(OTelPlugin.class);
        return plugins;
    }

    @Override
    protected boolean requiresDataSetup() {
        return false;
    }

    @Override
    protected Set<String> excludeTemplates() {
        // OTelPlugin's live registry listener races with wipe() if templates are deleted between
        // tests; excluding them prevents the deletion and keeps wipe() from failing.
        return Set.of(
            "profiling-events-otel@mappings",
            "profiling-hosts-otel@mappings",
            "profiling-stacktraces-otel@mappings",
            "profiling-stackframes-otel@mappings",
            "profiling-executables-otel@mappings",
            "profiling-events-otel@template",
            "profiling-hosts-otel@template",
            "profiling-stacktraces-otel@template",
            "profiling-stackframes-otel@template",
            "profiling-executables-otel@template"
        );
    }

    @Before
    public void setupOtelData() throws Exception {
        assertBusy(() -> {
            ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            assertTrue(
                "OTel profiling templates not yet installed",
                state.metadata().getProject().templatesV2().containsKey("profiling-events-otel@template")
            );
        });

        for (String file : List.of(
            "data/profiling-events-otel.ndjson",
            "data/profiling-stacktraces-otel.ndjson",
            "data/profiling-stackframes-otel.ndjson",
            "data/profiling-executables-otel.ndjson",
            "data/profiling-hosts-otel.ndjson"
        )) {
            byte[] bulkData = read(file);
            BulkResponse response = client().prepareBulk().add(bulkData, 0, bulkData.length, XContentType.JSON).get();
            if (response.hasFailures()) {
                fail(file + " bulk failures: " + response.buildFailureMessage());
            }
        }

        refresh();
        forceMerge();
    }

    private GetStackTracesRequest otelRequest() throws Exception {
        GetStackTracesRequest request = new GetStackTracesRequest();
        // tag::noformat
        try (XContentParser parser = createParser(XContentFactory.jsonBuilder()
            .startObject()
                .field("sample_size", 1000)
                .field("schema", "otel")
            .endObject()
        )) {
            request.parseXContent(parser);
        }
        // end::noformat
        return request;
    }

    public void testGetStackTracesFromOtelSchemaGroupedByServiceName() throws Exception {
        GetStackTracesRequest request = new GetStackTracesRequest();
        // tag::noformat
        try (XContentParser parser = createParser(XContentFactory.jsonBuilder()
            .startObject()
                .field("sample_size", 1000)
                .field("schema", "otel")
                .field("aggregation_fields", new String[] { "service.name" })
            .endObject()
        )) {
            request.parseXContent(parser);
        }
        // end::noformat
        request.setAdjustSampleCount(true);

        GetStackTracesResponse response = client().execute(GetStackTracesAction.INSTANCE, request).get();
        assertEquals(45, response.getTotalSamples());
        assertEquals(1821, response.getTotalFrames());

        assertNotNull(response.getStackTraceEvents());
        TraceEventID traceEventID = new TraceEventID(
            "",
            "497295213074376",
            "8457605156473051743",
            "L7kj7UvlKbT-vN73el4faQ",
            TransportGetStackTracesAction.DEFAULT_SAMPLING_FREQUENCY
        );
        assertEquals(2L, response.getStackTraceEvents().get(traceEventID).count);
        assertEquals(Long.valueOf(2L), response.getStackTraceEvents().get(traceEventID).subGroups.getCount("basket"));
    }

    public void testGetStackTracesFromOtelSchemaUnfiltered() throws Exception {
        GetStackTracesRequest request = otelRequest();
        request.setAdjustSampleCount(true);

        GetStackTracesResponse response = client().execute(GetStackTracesAction.INSTANCE, request).get();
        assertEquals(45, response.getTotalSamples());
        assertEquals(1821, response.getTotalFrames());

        assertNotNull(response.getStackTraces());
        StackTrace stackTrace = response.getStackTraces().get("L7kj7UvlKbT-vN73el4faQ");
        assertNotNull("Expected trace L7kj7UvlKbT-vN73el4faQ to be present", stackTrace);
        assertEquals(18, stackTrace.addressOrLines.length);
        assertEquals(18, stackTrace.fileIds.length);
        assertEquals(18, stackTrace.frameIds.length);
        assertEquals(18, stackTrace.typeIds.length);
        // subGroups is only populated when a groupBy field is requested
        assertNull(stackTrace.subGroups);

        assertNotNull(response.getStackFrames());
        StackFrame stackFrame = response.getStackFrames().get("8NlMClggx8jaziUTJXlmWAAAAAAAAIYI");
        assertNotNull("Expected frame 8NlMClggx8jaziUTJXlmWAAAAAAAAIYI to be present", stackFrame);
        assertEquals(List.of("start_thread"), stackFrame.functionName);
        // inlined frames: StackFrame.fromOtelSource must handle multi-value function.name lists
        StackFrame inlinedFrame = response.getStackFrames().get("fhsEKXDuxJ-jIJrZpdRuSAAAAAAAAFtj");
        assertNotNull("Expected inlined frame fhsEKXDuxJ-jIJrZpdRuSAAAAAAAAFtj to be present", inlinedFrame);
        assertEquals(List.of("deflate", "deflate"), inlinedFrame.functionName);

        assertNotNull(response.getExecutables());
        assertEquals("vmlinux", response.getExecutables().get("lHp5_WAgpLy2alrUVab6HA"));
    }
}
