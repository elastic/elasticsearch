/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.ingest.common;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

// Ideally I like this test to live in the server module, but otherwise a large part of the ScriptProcessor
// ends up being copied into this test.
@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class IngestRestartIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestCommonPlugin.class, CustomScriptPlugin.class);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
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
        client().admin().cluster().preparePutPipeline(pipelineId, new BytesArray(Strings.format("""
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
            }""", MockScriptEngine.NAME)), XContentType.JSON).get();

        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareIndex("index")
                .setId("1")
                .setSource("x", 0)
                .setPipeline(pipelineId)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get()
        );
        assertTrue(e.getMessage().contains("this script always fails"));

        NodesStatsResponse r = client().admin().cluster().prepareNodesStats(internalCluster().getNodeNames()).setIngest(true).get();
        int nodeCount = r.getNodes().size();
        for (int k = 0; k < nodeCount; k++) {
            List<IngestStats.ProcessorStat> stats = r.getNodes().get(k).getIngestStats().processorStats().get(pipelineId);
            for (IngestStats.ProcessorStat st : stats) {
                assertThat(st.stats().ingestCurrent(), greaterThanOrEqualTo(0L));
            }
        }
    }

    public void testScriptDisabled() throws Exception {
        String pipelineIdWithoutScript = randomAlphaOfLengthBetween(5, 10);
        String pipelineIdWithScript = pipelineIdWithoutScript + "_script";
        internalCluster().startNode();

        BytesReference pipelineWithScript = new BytesArray(Strings.format("""
            {
              "processors": [ { "script": { "lang": "%s", "source": "my_script" } } ]
            }""", MockScriptEngine.NAME));
        BytesReference pipelineWithoutScript = new BytesArray("""
            {
              "processors": [ { "set": { "field": "y", "value": 0 } } ]
            }""");

        Consumer<String> checkPipelineExists = (id) -> assertThat(
            client().admin().cluster().prepareGetPipeline(id).get().pipelines().get(0).getId(),
            equalTo(id)
        );

        client().admin().cluster().preparePutPipeline(pipelineIdWithScript, pipelineWithScript, XContentType.JSON).get();
        client().admin().cluster().preparePutPipeline(pipelineIdWithoutScript, pipelineWithoutScript, XContentType.JSON).get();

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

        client().prepareIndex("index")
            .setId("1")
            .setSource("x", 0)
            .setPipeline(pipelineIdWithoutScript)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> client().prepareIndex("index")
                .setId("2")
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

        client().admin().cluster().preparePutStoredScript().setId("1").setContent(new BytesArray(Strings.format("""
            {"script": {"lang": "%s", "source": "my_script"} }
            """, MockScriptEngine.NAME)), XContentType.JSON).get();
        BytesReference pipeline = new BytesArray("""
            {
              "processors" : [
                  {"set" : {"field": "y", "value": 0}},
                  {"script" : {"id": "1"}}
              ]
            }""");
        client().admin().cluster().preparePutPipeline("_id", pipeline, XContentType.JSON).get();

        client().prepareIndex("index")
            .setId("1")
            .setSource("x", 0)
            .setPipeline("_id")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

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

        client().prepareIndex("index")
            .setId("2")
            .setSource("x", 0)
            .setPipeline("_id")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        source = client().prepareGet("index", "2").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
        assertThat(source.get("z"), equalTo(0));
    }

    public void testWithDedicatedIngestNode() throws Exception {
        String node = internalCluster().startNode();
        String ingestNode = internalCluster().startNode(onlyRole(DiscoveryNodeRole.INGEST_ROLE));

        BytesReference pipeline = new BytesArray("""
            {
              "processors" : [
                  {"set" : {"field": "y", "value": 0}}
              ]
            }""");
        client().admin().cluster().preparePutPipeline("_id", pipeline, XContentType.JSON).get();

        client().prepareIndex("index")
            .setId("1")
            .setSource("x", 0)
            .setPipeline("_id")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

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

}
