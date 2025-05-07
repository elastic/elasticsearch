/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.ingest.common;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric.INGEST;
import static org.elasticsearch.action.admin.cluster.storedscripts.StoredScriptIntegTestUtils.putJsonStoredScript;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

// Ideally I like this test to live in the server module, but otherwise a large part of the ScriptProcessor
// ends up being copied into this test.
@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class IngestRestartIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestCommonPlugin.class, CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of("my_script", ctx -> {
                ctx.put("z", 0);
                return null;
            }, "throwing_script", ctx -> { throw new RuntimeException("this script always fails"); });
        }
    }

    public void testFailureInConditionalProcessor() {
        internalCluster().ensureAtLeastNumDataNodes(1);
        internalCluster().startMasterOnlyNode();
        final String pipelineId = "foo";
        putJsonPipeline(pipelineId, Strings.format("""
            {
              "processors": [
                {
                  "set": {
                    "field": "any_field",
                    "value": "any_value"
                  }
                },
                {
                  "set": {
                    "if": {
                      "lang": "%s",
                      "source": "throwing_script"
                    },
                    "field": "any_field2",
                    "value": "any_value2"
                  }
                }
              ]
            }""", MockScriptEngine.NAME));

        Exception e = expectThrows(
            Exception.class,
            () -> prepareIndex("index").setId("1")
                .setSource("x", 0)
                .setPipeline(pipelineId)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get()
        );
        assertTrue(e.getMessage().contains("this script always fails"));

        NodesStatsResponse r = clusterAdmin().prepareNodesStats(internalCluster().getNodeNames()).setIngest(true).get();
        int nodeCount = r.getNodes().size();
        for (int k = 0; k < nodeCount; k++) {
            List<IngestStats.ProcessorStat> stats = r.getNodes()
                .get(k)
                .getIngestStats()
                .processorStats()
                .get(ProjectId.DEFAULT)
                .get(pipelineId);
            for (IngestStats.ProcessorStat st : stats) {
                assertThat(st.stats().ingestCurrent(), greaterThanOrEqualTo(0L));
            }
        }
    }

    public void testScriptDisabled() throws Exception {
        String pipelineIdWithoutScript = randomAlphaOfLengthBetween(5, 10);
        String pipelineIdWithScript = pipelineIdWithoutScript + "_script";
        internalCluster().startNode();

        putJsonPipeline(pipelineIdWithScript, Strings.format("""
            {
              "processors": [ { "script": { "lang": "%s", "source": "my_script" } } ]
            }""", MockScriptEngine.NAME));
        putJsonPipeline(pipelineIdWithoutScript, """
            {
              "processors": [ { "set": { "field": "y", "value": 0 } } ]
            }""");

        Consumer<String> checkPipelineExists = (id) -> assertThat(getPipelines(id).pipelines().get(0).getId(), equalTo(id));

        checkPipelineExists.accept(pipelineIdWithScript);
        checkPipelineExists.accept(pipelineIdWithoutScript);

        internalCluster().restartNode(internalCluster().getMasterName(), new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put("script.allowed_types", "none").build();
            }

        });

        checkPipelineExists.accept(pipelineIdWithoutScript);
        checkPipelineExists.accept(pipelineIdWithScript);

        prepareIndex("index").setId("1")
            .setSource("x", 0)
            .setPipeline(pipelineIdWithoutScript)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> prepareIndex("index").setId("2")
                .setSource("x", 0)
                .setPipeline(pipelineIdWithScript)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get()
        );
        assertThat(
            exception.getMessage(),
            equalTo(
                "pipeline with id ["
                    + pipelineIdWithScript
                    + "] could not be loaded, caused by "
                    + "[org.elasticsearch.ElasticsearchParseException: Error updating pipeline with id ["
                    + pipelineIdWithScript
                    + "]; "
                    + "org.elasticsearch.ElasticsearchException: java.lang.IllegalArgumentException: cannot execute [inline] scripts; "
                    + "java.lang.IllegalArgumentException: cannot execute [inline] scripts]"
            )
        );

        Map<String, Object> source = client().prepareGet("index", "1").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
    }

    public void testPipelineWithScriptProcessorThatHasStoredScript() throws Exception {
        internalCluster().startNode();

        putJsonStoredScript("1", Strings.format("""
            {"script": {"lang": "%s", "source": "my_script"} }
            """, MockScriptEngine.NAME));
        putJsonPipeline("_id", """
            {
              "processors" : [
                  {"set" : {"field": "y", "value": 0}},
                  {"script" : {"id": "1"}}
              ]
            }""");

        prepareIndex("index").setId("1").setSource("x", 0).setPipeline("_id").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        Map<String, Object> source = client().prepareGet("index", "1").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
        assertThat(source.get("z"), equalTo(0));

        // Prior to making this ScriptService implement ClusterStateApplier instead of ClusterStateListener,
        // pipelines with a script processor failed to load causing these pipelines and pipelines that were
        // supposed to load after these pipelines to not be available during ingestion, which then causes
        // the next index request in this test to fail.
        internalCluster().fullRestart();
        ensureYellow("index");

        prepareIndex("index").setId("2").setSource("x", 0).setPipeline("_id").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        source = client().prepareGet("index", "2").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
        assertThat(source.get("z"), equalTo(0));
    }

    public void testWithDedicatedIngestNode() throws Exception {
        String node = internalCluster().startNode();
        String ingestNode = internalCluster().startNode(onlyRole(DiscoveryNodeRole.INGEST_ROLE));

        putJsonPipeline("_id", """
            {
              "processors" : [
                  {"set" : {"field": "y", "value": 0}}
              ]
            }""");

        prepareIndex("index").setId("1").setSource("x", 0).setPipeline("_id").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        Map<String, Object> source = client().prepareGet("index", "1").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));

        logger.info("Stopping");
        internalCluster().restartNode(node, new InternalTestCluster.RestartCallback());

        client(ingestNode).prepareIndex("index")
            .setId("2")
            .setSource("x", 0)
            .setPipeline("_id")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        source = client(ingestNode).prepareGet("index", "2").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
    }

    public void testDefaultPipelineWaitForClusterStateRecovered() throws Exception {
        internalCluster().startNode();

        putJsonPipeline("test_pipeline", """
            {
              "processors" : [
                {
                  "set": {
                    "field": "value",
                    "value": 42
                  }
                }
              ]
            }""");

        final TimeValue timeout = TimeValue.timeValueSeconds(10);
        client().admin().indices().preparePutTemplate("pipeline_template").setPatterns(Collections.singletonList("*")).setSettings("""
            {
              "index" : {
                 "default_pipeline" : "test_pipeline"
              }
            }
            """, XContentType.JSON).get(timeout);

        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(), "2").build();
            }

            @Override
            public boolean validateClusterForming() {
                return randomBoolean();
            }
        });

        // this one should fail
        assertThat(
            expectThrows(
                ClusterBlockException.class,
                () -> prepareIndex("index").setId("fails")
                    .setSource("x", 1)
                    .setTimeout(TimeValue.timeValueMillis(100)) // 100ms, to fail quickly
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .get(timeout)
            ).getMessage(),
            equalTo("blocked by: [SERVICE_UNAVAILABLE/1/state not recovered / initialized];")
        );

        // but this one should pass since it has a longer timeout
        final PlainActionFuture<DocWriteResponse> future = new PlainActionFuture<>();
        prepareIndex("index").setId("passes1")
            .setSource("x", 2)
            .setTimeout(TimeValue.timeValueSeconds(60)) // wait for second node to start in below
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute(future);

        // so the cluster state can be recovered
        internalCluster().startNode(Settings.builder().put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(), "1"));
        ensureYellow("index");

        final DocWriteResponse indexResponse = future.actionGet(timeout);
        assertThat(indexResponse.status(), equalTo(RestStatus.CREATED));
        assertThat(indexResponse.getResult(), equalTo(DocWriteResponse.Result.CREATED));

        prepareIndex("index").setId("passes2").setSource("x", 3).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // successfully indexed documents should have the value field set by the pipeline
        Map<String, Object> source = client().prepareGet("index", "passes1").get(timeout).getSource();
        assertThat(source.get("x"), equalTo(2));
        assertThat(source.get("value"), equalTo(42));

        source = client().prepareGet("index", "passes2").get(timeout).getSource();
        assertThat(source.get("x"), equalTo(3));
        assertThat(source.get("value"), equalTo(42));

        // and make sure this failed doc didn't get through
        source = client().prepareGet("index", "fails").get(timeout).getSource();
        assertNull(source);
    }

    /**
     * This test is for confirming that forwarded bulk requests do not use system_write thread pool
     * for non-system indexes. Before this fix, we were using system_write thread pool for all forwarded
     * bulk requests causing the system_write thread pool to get overloaded.
     */
    public void testForwardBulkWithSystemWritePoolDisabled() throws Exception {
        // Create a node with master only role and a node with ingest role
        final String masterOnlyNode = internalCluster().startMasterOnlyNode();
        final String ingestNode = internalCluster().startNode();

        ensureStableCluster(2);

        // Create Bulk Request
        createIndex("index");

        putJsonPipeline("_id", """
            {
              "processors" : [
                  {"set" : {"field": "y", "value": 0}}
              ]
            }""");

        int numRequests = scaledRandomIntBetween(32, 128);
        BulkRequest bulkRequest = new BulkRequest();
        BulkResponse response;
        for (int i = 0; i < numRequests; i++) {
            IndexRequest indexRequest = new IndexRequest("index").id(Integer.toString(i)).setPipeline("_id");
            indexRequest.source(Requests.INDEX_CONTENT_TYPE, "x", 1);
            bulkRequest.add(indexRequest);
        }
        assertThat(numRequests, equalTo(bulkRequest.requests().size()));

        // Block system_write thread pool on the ingest node
        final ThreadPool ingestNodeThreadPool = internalCluster().getInstance(ThreadPool.class, ingestNode);
        final var blockingLatch = new CountDownLatch(1);
        try {
            blockSystemWriteThreadPool(blockingLatch, ingestNodeThreadPool);
            // Send bulk request to master only node, so it will forward it to the ingest node.
            response = safeGet(client(masterOnlyNode).bulk(bulkRequest));
        } finally {
            blockingLatch.countDown();
        }

        // Make sure the requests are processed (even though we blocked system_write thread pool above).
        assertThat(response.getItems().length, equalTo(numRequests));
        assertFalse(response.hasFailures());

        // Check Node Ingest stats
        NodesStatsResponse nodesStatsResponse = clusterAdmin().nodesStats(new NodesStatsRequest(ingestNode).addMetric(INGEST)).actionGet();
        assertThat(nodesStatsResponse.getNodes().size(), equalTo(1));

        NodeStats stats = nodesStatsResponse.getNodes().get(0);
        assertThat(stats.getIngestStats().totalStats().ingestCount(), equalTo((long) numRequests));
        assertThat(stats.getIngestStats().totalStats().ingestFailedCount(), equalTo(0L));
        final var pipelineStats = stats.getIngestStats().pipelineStats().get(0);
        assertThat(pipelineStats.pipelineId(), equalTo("_id"));
        assertThat(pipelineStats.stats().ingestCount(), equalTo((long) numRequests));

        MultiGetResponse docListResponse = safeGet(
            client().prepareMultiGet().addIds("index", IntStream.range(0, numRequests).mapToObj(String::valueOf).toList()).execute()
        );

        assertThat(docListResponse.getResponses().length, equalTo(numRequests));
        Map<String, Object> document;
        for (int i = 0; i < numRequests; i++) {
            document = docListResponse.getResponses()[i].getResponse().getSourceAsMap();
            assertThat(document.get("y"), equalTo(0));
        }
    }

    private void blockSystemWriteThreadPool(CountDownLatch blockingLatch, ThreadPool threadPool) {
        assertThat(blockingLatch.getCount(), greaterThan(0L));
        final var executor = threadPool.executor(ThreadPool.Names.SYSTEM_WRITE);
        // Add tasks repeatedly until we get an EsRejectedExecutionException which indicates that the threadpool and its queue are full.
        expectThrows(EsRejectedExecutionException.class, () -> {
            // noinspection InfiniteLoopStatement
            while (true) {
                executor.execute(() -> safeAwait(blockingLatch));
            }
        });
    }
}
