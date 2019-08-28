/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

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
            return Collections.singletonMap("my_script", ctx -> {
                ctx.put("z", 0);
                return null;
            });
        }
    }

    public void testScriptDisabled() throws Exception {
        String pipelineIdWithoutScript = randomAlphaOfLengthBetween(5, 10);
        String pipelineIdWithScript = pipelineIdWithoutScript + "_script";
        internalCluster().startNode();

        BytesReference pipelineWithScript = new BytesArray("{\n" +
            "  \"processors\" : [\n" +
            "      {\"script\" : {\"lang\": \"" + MockScriptEngine.NAME + "\", \"source\": \"my_script\"}}\n" +
            "  ]\n" +
            "}");
        BytesReference pipelineWithoutScript = new BytesArray("{\n" +
            "  \"processors\" : [\n" +
            "      {\"set\" : {\"field\": \"y\", \"value\": 0}}\n" +
            "  ]\n" +
            "}");

        Consumer<String> checkPipelineExists = (id) -> assertThat(client().admin().cluster().prepareGetPipeline(id)
                .get().pipelines().get(0).getId(), equalTo(id));

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

        client().prepareIndex("index", "doc", "1")
            .setSource("x", 0)
            .setPipeline(pipelineIdWithoutScript)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> client().prepareIndex("index", "doc", "2")
                .setSource("x", 0)
                .setPipeline(pipelineIdWithScript)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get());
        assertThat(exception.getHeaderKeys(), equalTo(Sets.newHashSet("processor_type")));
        assertThat(exception.getHeader("processor_type"), equalTo(Arrays.asList("unknown")));
        assertThat(exception.getRootCause().getMessage(),
            equalTo("pipeline with id [" + pipelineIdWithScript + "] could not be loaded, caused by " +
                "[ElasticsearchParseException[Error updating pipeline with id [" + pipelineIdWithScript + "]]; " +
                "nested: ElasticsearchException[java.lang.IllegalArgumentException: cannot execute [inline] scripts]; " +
                "nested: IllegalArgumentException[cannot execute [inline] scripts];; " +
                "ElasticsearchException[java.lang.IllegalArgumentException: cannot execute [inline] scripts]; " +
                "nested: IllegalArgumentException[cannot execute [inline] scripts];; java.lang.IllegalArgumentException: " +
                "cannot execute [inline] scripts]"));

        Map<String, Object> source = client().prepareGet("index", "doc", "1").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
    }

    public void testPipelineWithScriptProcessorThatHasStoredScript() throws Exception {
        internalCluster().startNode();

        client().admin().cluster().preparePutStoredScript()
                .setId("1")
                .setContent(new BytesArray("{\"script\": {\"lang\": \"" + MockScriptEngine.NAME +
                        "\", \"source\": \"my_script\"} }"), XContentType.JSON)
                .get();
        BytesReference pipeline = new BytesArray("{\n" +
                "  \"processors\" : [\n" +
                "      {\"set\" : {\"field\": \"y\", \"value\": 0}},\n" +
                "      {\"script\" : {\"id\": \"1\"}}\n" +
                "  ]\n" +
                "}");
        client().admin().cluster().preparePutPipeline("_id", pipeline, XContentType.JSON).get();

        client().prepareIndex("index", "doc", "1")
                .setSource("x", 0)
                .setPipeline("_id")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        Map<String, Object> source = client().prepareGet("index", "doc", "1").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
        assertThat(source.get("z"), equalTo(0));

        // Prior to making this ScriptService implement ClusterStateApplier instead of ClusterStateListener,
        // pipelines with a script processor failed to load causing these pipelines and pipelines that were
        // supposed to load after these pipelines to not be available during ingestion, which then causes
        // the next index request in this test to fail.
        internalCluster().fullRestart();
        ensureYellow("index");

        client().prepareIndex("index", "doc", "2")
                .setSource("x", 0)
                .setPipeline("_id")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        source = client().prepareGet("index", "doc", "2").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
        assertThat(source.get("z"), equalTo(0));
    }

    public void testDefaultPipelineBeforeCSRecovered() throws Exception {
        internalCluster().startNode();

        BytesReference pipeline = new BytesArray("{\n" +
            "  \"processors\" : [\n" +
            "    {\n" +
            "      \"remove\": {\n" +
            "        \"field\": \"_type\"\n" +
            "      }\n" +
            "    }" +
            "  ]\n" +
            "}");
        client().admin().cluster().preparePutPipeline("test_pipeline", pipeline, XContentType.JSON).get();
        client().admin().indices().preparePutTemplate("pipeline_template")
            .setPatterns(Collections.singletonList("*"))
            .setSettings(
                "{\n" +
                "  \"index\" : {\n" +
                "     \"default_pipeline\" : \"test_pipeline\"" +
                "   }\n" +
                "}\n", XContentType.JSON).get();


        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put(GatewayService.RECOVER_AFTER_NODES_SETTING.getKey(), "2").build();
            }

            @Override
            public boolean validateClusterForming() {
                return false;
            }
        });

        CountDownLatch latch = new CountDownLatch(1);

        // this one should fail
        assertThat(expectThrows(ClusterBlockException.class, () -> client().prepareIndex("index", "foo", "fails")
            .setSource("x", 1)
            .setTimeout(TimeValue.timeValueMillis(100)) // 100ms, to fail quickly
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get()).getMessage(), equalTo("blocked by: [SERVICE_UNAVAILABLE/1/state not recovered / initialized];"));

        // but this one should pass since it has a longer timeout
        client().prepareIndex("index", "foo", "passes1")
            .setSource("x", 2)
            .setTimeout(TimeValue.timeValueSeconds(60)) // wait for second node to start
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute(new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    assertThat(indexResponse.status(), equalTo(RestStatus.CREATED));
                    assertThat(indexResponse.getResult(), equalTo(DocWriteResponse.Result.CREATED));
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not have failed with exception: " + e.getMessage());
                }
            });

        // so the cluster state can be recovered
        internalCluster()
            .startNode(Settings.builder().put(GatewayService.RECOVER_AFTER_NODES_SETTING.getKey(), "1"));
        ensureYellow("index");

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        client().prepareIndex("index", "bar", "passes2")
            .setSource("x", 3)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        client().admin().indices().prepareRefresh("index").get();

        // note that the types are _doc not `foo` or `bar`
        Map<String, Object> source = client().prepareGet("index", "_doc", "passes1").get().getSource();
        assertThat(source.get("x"), equalTo(2));

        source = client().prepareGet("index", "_doc", "passes2").get().getSource();
        assertThat(source.get("x"), equalTo(3));

        // and make sure this failed doc didn't get through
        source = client().prepareGet("index", "foo", "fails").get().getSource();
        assertNull(source);
        source = client().prepareGet("index", "_doc", "fails").get().getSource();
        assertNull(source);

    }

    public void testWithDedicatedIngestNode() throws Exception {
        String node = internalCluster().startNode();
        String ingestNode = internalCluster().startNode(Settings.builder()
                .put("node.master", false)
                .put("node.data", false)
        );

        BytesReference pipeline = new BytesArray("{\n" +
                "  \"processors\" : [\n" +
                "      {\"set\" : {\"field\": \"y\", \"value\": 0}}\n" +
                "  ]\n" +
                "}");
        client().admin().cluster().preparePutPipeline("_id", pipeline, XContentType.JSON).get();

        client().prepareIndex("index", "doc", "1")
                .setSource("x", 0)
                .setPipeline("_id")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        Map<String, Object> source = client().prepareGet("index", "doc", "1").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));

        logger.info("Stopping");
        internalCluster().restartNode(node, new InternalTestCluster.RestartCallback());

        client(ingestNode).prepareIndex("index", "doc", "2")
                .setSource("x", 0)
                .setPipeline("_id")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        source = client(ingestNode).prepareGet("index", "doc", "2").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
    }

}
