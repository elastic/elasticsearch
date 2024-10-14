/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.FailureStoreMetrics;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.ingest.FakeProcessor;
import org.elasticsearch.ingest.IngestInfo;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.ProcessorInfo;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.reservedstate.service.ReservedClusterStateService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests that the ReservedPipelineAction does validation, can add and remove pipelines
 */
public class ReservedPipelineActionTests extends ESTestCase {
    private static final IngestPlugin DUMMY_PLUGIN = new IngestPlugin() {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> processors = new HashMap<>();
            processors.put("set", (factories, tag, description, config) -> {
                String field = (String) config.remove("field");
                String value = (String) config.remove("value");
                return new FakeProcessor("set", tag, description, (ingestDocument) -> ingestDocument.setFieldValue(field, value));
            });

            return processors;
        }
    };

    private ThreadPool threadPool;
    private IngestService ingestService;
    private FileSettingsService fileSettingsService;

    @Before
    public void setup() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        Client client = mock(Client.class);
        ingestService = new IngestService(
            mock(ClusterService.class),
            threadPool,
            null,
            null,
            null,
            Collections.singletonList(DUMMY_PLUGIN),
            client,
            null,
            DocumentParsingProvider.EMPTY_INSTANCE,
            FailureStoreMetrics.NOOP
        );
        Map<String, Processor.Factory> factories = ingestService.getProcessorFactories();
        assertTrue(factories.containsKey("set"));
        assertEquals(1, factories.size());

        DiscoveryNode discoveryNode = DiscoveryNodeUtils.builder("_node_id").roles(emptySet()).build();

        NodeInfo nodeInfo = new NodeInfo(
            Build.current().version(),
            TransportVersion.current(),
            IndexVersion.current(),
            Map.of(),
            Build.current(),
            discoveryNode,
            Settings.EMPTY,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new IngestInfo(Collections.singletonList(new ProcessorInfo("set"))),
            null,
            null
        );
        NodesInfoResponse response = new NodesInfoResponse(new ClusterName("elasticsearch"), List.of(nodeInfo), List.of());

        var clusterService = spy(
            new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            )
        );

        fileSettingsService = spy(
            new FileSettingsService(clusterService, mock(ReservedClusterStateService.class), newEnvironment(Settings.EMPTY))
        );
    }

    private TransformState processJSON(ReservedPipelineAction action, TransformState prevState, String json) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return action.transform(action.fromXContent(parser), prevState);
        }
    }

    public void testAddRemoveIngestPipeline() throws Exception {
        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        ReservedPipelineAction action = new ReservedPipelineAction();

        String emptyJSON = "";

        TransformState updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
        assertEquals(prevState.state(), updatedState.state());

        String json = """
            {
               "my_ingest_pipeline": {
                   "description": "_description",
                   "processors": [
                      {
                        "set" : {
                          "field": "_field",
                          "value": "_value"
                        }
                      }
                   ]
               },
               "my_ingest_pipeline_1": {
                   "description": "_description",
                   "processors": [
                      {
                        "set" : {
                          "field": "_field",
                          "value": "_value"
                        }
                      }
                   ]
               }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, json);
        assertThat(updatedState.keys(), containsInAnyOrder("my_ingest_pipeline", "my_ingest_pipeline_1"));

        String halfJSON = """
            {
               "my_ingest_pipeline_1": {
                   "description": "_description",
                   "processors": [
                      {
                        "set" : {
                          "field": "_field",
                          "value": "_value"
                        }
                      }
                   ]
               }
            }""";

        updatedState = processJSON(action, prevState, halfJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("my_ingest_pipeline_1"));

        updatedState = processJSON(action, prevState, emptyJSON);
        assertThat(updatedState.keys(), empty());
    }
}
