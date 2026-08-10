/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.yaml.YamlXContent;
import org.elasticsearch.xpack.core.template.ResourceUtils;
import org.elasticsearch.xpack.oteldata.OTelIndexTemplateRegistry;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Tests the OTel read path with data indexed into {@code *.otel-*} data streams.
 *
 * OTel profiling templates are installed directly from the otel-data classpath rather than via
 * OTelPlugin in nodePlugins(). Loading OTelPlugin at node startup fails because YamlTemplateRegistry
 * picks up the x-pack-core test resources.yaml (which lists test-only templates) before the
 * otel-data one on the shared test classpath.
 */
public class GetStackTracesOtelActionIT extends ProfilingTestCase {

    private static final List<String> PROFILING_COMPONENT_TEMPLATES = List.of(
        "profiling-events-otel@mappings",
        "profiling-hosts-otel@mappings",
        "profiling-stacktraces-otel@mappings",
        "profiling-stackframes-otel@mappings",
        "profiling-executables-otel@mappings"
    );

    private static final List<String> PROFILING_INDEX_TEMPLATES = List.of(
        "profiling-events-otel@template",
        "profiling-hosts-otel@template",
        "profiling-stacktraces-otel@template",
        "profiling-stackframes-otel@template",
        "profiling-executables-otel@template"
    );

    @Override
    protected boolean requiresDataSetup() {
        // OTel IT manages its own data; skip ECS index creation from the base class.
        return false;
    }

    @Before
    public void setupOtelData() throws Exception {
        installOtelProfilingTemplates();

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

    /**
     * Reads each profiling OTel component and index template YAML from the {@code otel-data}
     * plugin's classpath and installs it via the cluster API. Individual template resource paths
     * such as {@code /component-templates/profiling-events-otel@mappings.yaml} are unique to
     * otel-data and do not conflict with x-pack-core test resources.
     */
    private void installOtelProfilingTemplates() throws Exception {
        for (String name : PROFILING_COMPONENT_TEMPLATES) {
            byte[] content = loadOtelResource("/component-templates/" + name + ".yaml");
            try (XContentParser parser = YamlXContent.yamlXContent.createParser(XContentParserConfiguration.EMPTY, content)) {
                ComponentTemplate template = ComponentTemplate.parse(parser);
                PutComponentTemplateAction.Request req = new PutComponentTemplateAction.Request(name);
                req.componentTemplate(template);
                assertAcked(client().execute(PutComponentTemplateAction.INSTANCE, req).actionGet());
            }
        }
        for (String name : PROFILING_INDEX_TEMPLATES) {
            byte[] content = loadOtelResource("/index-templates/" + name + ".yaml");
            try (XContentParser parser = YamlXContent.yamlXContent.createParser(XContentParserConfiguration.EMPTY, content)) {
                ComposableIndexTemplate template = ComposableIndexTemplate.parse(parser);
                TransportPutComposableIndexTemplateAction.Request req = new TransportPutComposableIndexTemplateAction.Request(name);
                req.indexTemplate(template);
                assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, req).actionGet());
            }
        }
    }

    private static byte[] loadOtelResource(String path) throws Exception {
        String raw = ResourceUtils.loadResource(OTelIndexTemplateRegistry.class, path);
        // Strip the version line so we don't need to know or pin the otel-data template version.
        // The template content (mappings, settings) is what matters for correctness; the version
        // field is only used for upgrade detection, which is irrelevant in a fresh test cluster.
        raw = raw.replaceFirst("(?m)^version:.*\\R", "");
        return raw.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Verifies that the {@code groupBy} sub-aggregation works through the OTel passthrough mapping.
     * In the OTel events schema {@code service.name} lives in {@code resource.attributes.service.name};
     * the passthrough field exposes it as a top-level {@code service.name} so the composite
     * aggregation can group on it without any OTel-specific branching in the action.
     */
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

    /**
     * Verifies the full OTel read path end-to-end: events aggregation, stacktrace mget
     * (via {@link StackTrace#fromOtelSource}), stackframe mget (via {@link StackFrame#fromOtelSource}),
     * executable mget, and host lookup (via {@link HostMetadata#fromOtelSource}).
     */
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
