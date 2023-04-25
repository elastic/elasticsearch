/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.application.utils.ingest.PipelineTemplateConfiguration;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.application.analytics.AnalyticsIngestPipelineRegistry.EVENT_DATA_STREAM_INGEST_PIPELINE_NAME;
import static org.elasticsearch.xpack.application.analytics.AnalyticsTemplateRegistry.REGISTRY_VERSION;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class AnalyticsIngestPipelineRegistryTests extends ESTestCase {
    private AnalyticsIngestPipelineRegistry registry;
    private ThreadPool threadPool;
    private VerifyingClient client;

    @Before
    public void createRegistryAndClient() {
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        registry = new AnalyticsIngestPipelineRegistry(clusterService, threadPool, client);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testThatNonExistingPipelinesAreAddedImmediately() throws Exception {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), nodes);

        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> verifyIngestPipelinesInstalled(calledTimes, action, request, listener));
        registry.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(registry.getIngestPipelineConfigs().size())));
    }

    public void testIngestPipelinesAlreadyExists() {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        client.setVerifier((action, request, listener) -> {
            if (action instanceof PutPipelineAction) {
                fail("if the pipeline already exists it should not be re-put");
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        Map<String, Integer> existingPipelines = registry.getIngestPipelineConfigs()
            .stream()
            .collect(Collectors.toMap(PipelineTemplateConfiguration::getId, PipelineTemplateConfiguration::getVersion));

        ClusterChangedEvent event = createClusterChangedEvent(existingPipelines, nodes);
        registry.clusterChanged(event);
    }

    public void testThatVersionedOldPipelinesAreUpgraded() throws Exception {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, Integer> existingPipelines = registry.getIngestPipelineConfigs()
            .stream()
            .collect(Collectors.toMap(PipelineTemplateConfiguration::getId, pipelineConfig -> pipelineConfig.getVersion() - 1));

        ClusterChangedEvent event = createClusterChangedEvent(existingPipelines, nodes);

        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> verifyIngestPipelinesInstalled(calledTimes, action, request, listener));
        registry.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(registry.getIngestPipelineConfigs().size())));
    }

    public void testThatNewerPipelinesAreNotUpgraded() throws Exception {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, Integer> existingPipelines = registry.getIngestPipelineConfigs()
            .stream()
            .collect(Collectors.toMap(PipelineTemplateConfiguration::getId, pipelineConfig -> pipelineConfig.getVersion() + 1));

        ClusterChangedEvent event = createClusterChangedEvent(existingPipelines, nodes);

        client.setVerifier((action, request, listener) -> {
            if (action instanceof PutPipelineAction) {
                fail("if the pipeline already exists it should not be re-put");
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        registry.clusterChanged(event);
    }

    public void testThatMissingMasterNodeDoesNothing() {
        DiscoveryNode localNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").add(localNode).build();

        client.setVerifier((a, r, l) -> {
            fail("if the master is missing nothing should happen");
            return null;
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), nodes);
        registry.clusterChanged(event);
    }

    public static class VerifyingClient extends NoOpClient {
        private TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier = (a, r, l) -> {
            fail("verifier not set");
            return null;
        };

        VerifyingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            try {
                listener.onResponse((Response) verifier.apply(action, request, listener));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        public VerifyingClient setVerifier(TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier) {
            this.verifier = verifier;
            return this;
        }
    }

    private ActionResponse verifyIngestPipelinesInstalled(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        if (action instanceof PutPipelineAction) {
            calledTimes.incrementAndGet();
            assertThat(request, instanceOf(PutPipelineRequest.class));
            final PutPipelineRequest putRequest = (PutPipelineRequest) request;
            assertThat(putRequest.getId(), equalTo(EVENT_DATA_STREAM_INGEST_PIPELINE_NAME));
            Map<String, ?> requestContentMap = XContentHelper.convertToMap(putRequest.getSource(), false, putRequest.getXContentType())
                .v2();
            assertThat(requestContentMap.get("version"), equalTo(REGISTRY_VERSION));
            assertNotNull(listener);
            return AcknowledgedResponse.TRUE;
        } else {
            fail("client called with unexpected request:" + request.toString());
            return null;
        }
    }

    private ClusterChangedEvent createClusterChangedEvent(Map<String, Integer> existingIngestPipelines, DiscoveryNodes nodes) {
        ClusterState cs = createClusterState(existingIngestPipelines, nodes);
        ClusterChangedEvent realEvent = new ClusterChangedEvent(
            "created-from-test",
            cs,
            ClusterState.builder(new ClusterName("test")).build()
        );
        ClusterChangedEvent event = spy(realEvent);
        when(event.localNodeMaster()).thenReturn(nodes.isLocalNodeElectedMaster());

        return event;
    }

    private ClusterState createClusterState(Map<String, Integer> existingIngestPipelines, DiscoveryNodes nodes) {
        Map<String, PipelineConfiguration> pipelines = new HashMap<>();
        for (Map.Entry<String, Integer> e : existingIngestPipelines.entrySet()) {
            pipelines.put(e.getKey(), createMockPipelineConfiguration(e.getKey(), e.getValue()));
        }

        return ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder().transientSettings(Settings.EMPTY).putCustom(IngestMetadata.TYPE, new IngestMetadata(pipelines)).build()
            )
            .blocks(new ClusterBlocks.Builder().build())
            .nodes(nodes)
            .build();
    }

    private PipelineConfiguration createMockPipelineConfiguration(String pipelineId, int version) {
        try (XContentBuilder configBuilder = JsonXContent.contentBuilder().startObject().field("version", version).endObject()) {
            BytesReference config = BytesReference.bytes(configBuilder);
            return new PipelineConfiguration(pipelineId, config, configBuilder.contentType());
        } catch (IOException e) {
            return null;
        }
    }
}
