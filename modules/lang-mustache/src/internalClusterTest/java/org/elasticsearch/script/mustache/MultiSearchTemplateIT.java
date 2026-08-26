/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.MultiSearchTemplateResponse.Item;
import org.elasticsearch.search.DummyQueryParserPlugin;
import org.elasticsearch.search.FailBeforeCurrentVersionQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.AbstractSearchCancellationTestCase;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class MultiSearchTemplateIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MustachePlugin.class, DummyQueryParserPlugin.class, AbstractSearchCancellationTestCase.ScriptedBlockPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable real HTTP for REST-level header and channel-close tests
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.CCS_VERSION_CHECK_SETTING.getKey(), "true")
            .build();
    }

    public void testBasic() throws Exception {
        createIndex("msearch");
        final int numDocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders[i] = prepareIndex("msearch").setId(String.valueOf(i)).setSource("odd", (i % 2 == 0), "group", (i % 3));
        }
        indexRandom(true, indexRequestBuilders);

        final String template = Strings.toString(
            jsonBuilder().startObject()
                .startObject("query")
                .startObject("{{query_type}}")
                .field("{{field_name}}", "{{field_value}}")
                .endObject()
                .endObject()
                .endObject()
        );

        MultiSearchTemplateRequest multiRequest = new MultiSearchTemplateRequest();

        // Search #1
        SearchTemplateRequest search1 = new SearchTemplateRequest();
        search1.setRequest(new SearchRequest("msearch"));
        search1.setScriptType(ScriptType.INLINE);
        search1.setScript(template);

        Map<String, Object> params1 = new HashMap<>();
        params1.put("query_type", "match");
        params1.put("field_name", "odd");
        params1.put("field_value", true);
        search1.setScriptParams(params1);
        multiRequest.add(search1);

        // Search #2 (Simulate is true)
        SearchTemplateRequest search2 = new SearchTemplateRequest();
        search2.setRequest(new SearchRequest("msearch"));
        search2.setScriptType(ScriptType.INLINE);
        search2.setScript(template);
        search2.setSimulate(true);

        Map<String, Object> params2 = new HashMap<>();
        params2.put("query_type", "match_phrase_prefix");
        params2.put("field_name", "message");
        params2.put("field_value", "quick brown f");
        search2.setScriptParams(params2);
        multiRequest.add(search2);

        // Search #3
        SearchTemplateRequest search3 = new SearchTemplateRequest();
        search3.setRequest(new SearchRequest("msearch"));
        search3.setScriptType(ScriptType.INLINE);
        search3.setScript(template);
        search3.setSimulate(false);

        Map<String, Object> params3 = new HashMap<>();
        params3.put("query_type", "term");
        params3.put("field_name", "odd");
        params3.put("field_value", "false");
        search3.setScriptParams(params3);
        multiRequest.add(search3);

        // Search #4 (Fail because of unknown index)
        SearchTemplateRequest search4 = new SearchTemplateRequest();
        search4.setRequest(new SearchRequest("unknown"));
        search4.setScriptType(ScriptType.INLINE);
        search4.setScript(template);

        Map<String, Object> params4 = new HashMap<>();
        params4.put("query_type", "match");
        params4.put("field_name", "group");
        params4.put("field_value", "test");
        search4.setScriptParams(params4);
        multiRequest.add(search4);

        // Search #5 (Simulate is true)
        SearchTemplateRequest search5 = new SearchTemplateRequest();
        search5.setRequest(new SearchRequest("msearch"));
        search5.setScriptType(ScriptType.INLINE);
        search5.setScript("{{! ignore me }}{\"query\":{\"terms\":{\"group\":[{{#groups}}{{.}},{{/groups}}]}}}");
        search5.setSimulate(true);

        Map<String, Object> params5 = new HashMap<>();
        params5.put("groups", Arrays.asList(1, 2, 3));
        search5.setScriptParams(params5);
        multiRequest.add(search5);

        assertResponse(client().execute(MustachePlugin.MULTI_SEARCH_TEMPLATE_ACTION, multiRequest), response -> {
            assertThat(response.getResponses(), arrayWithSize(5));
            assertThat(response.getTook().millis(), greaterThan(0L));

            MultiSearchTemplateResponse.Item response1 = response.getResponses()[0];
            assertThat(response1.isFailure(), is(false));
            SearchTemplateResponse searchTemplateResponse1 = response1.getResponse();
            assertThat(searchTemplateResponse1.hasResponse(), is(true));
            assertHitCount(searchTemplateResponse1.getResponse(), (numDocs / 2) + (numDocs % 2));
            assertThat(searchTemplateResponse1.getSource().utf8ToString(), equalTo("""
                {"query":{"match":{"odd":"true"}}}"""));

            MultiSearchTemplateResponse.Item response2 = response.getResponses()[1];
            assertThat(response2.isFailure(), is(false));
            SearchTemplateResponse searchTemplateResponse2 = response2.getResponse();
            assertThat(searchTemplateResponse2.hasResponse(), is(false));
            assertThat(searchTemplateResponse2.getSource().utf8ToString(), equalTo("""
                {"query":{"match_phrase_prefix":{"message":"quick brown f"}}}"""));

            MultiSearchTemplateResponse.Item response3 = response.getResponses()[2];
            assertThat(response3.isFailure(), is(false));
            SearchTemplateResponse searchTemplateResponse3 = response3.getResponse();
            assertThat(searchTemplateResponse3.hasResponse(), is(true));
            assertHitCount(searchTemplateResponse3.getResponse(), (numDocs / 2));
            assertThat(searchTemplateResponse3.getSource().utf8ToString(), equalTo("""
                {"query":{"term":{"odd":"false"}}}"""));

            MultiSearchTemplateResponse.Item response4 = response.getResponses()[3];
            assertThat(response4.isFailure(), is(true));
            assertThat(response4.getFailure(), instanceOf(IndexNotFoundException.class));
            assertThat(response4.getFailure().getMessage(), equalTo("no such index [unknown]"));

            MultiSearchTemplateResponse.Item response5 = response.getResponses()[4];
            assertThat(response5.isFailure(), is(true));
            assertNull(response5.getResponse());
            assertThat(response5.getFailure(), instanceOf(XContentParseException.class));
        });
    }

    /**
     * Regression test: a large msearch/template request must not cause OOM on the coordinator.
     * <p>
     * The action charges the REQUEST circuit breaker for each rendered template source held in
     * memory before the inner searches execute. With a tight breaker, the first large render
     * trips it and all remaining slots receive {@link CircuitBreakingException} item failures
     * rather than silently accumulating until the node runs out of heap.
     * <p>
     * Render estimate = {@code 512 + source.length() + 2 × serialised(SearchSourceBuilder)}.
     * The ~16 KB template body produces a serialised builder of comparable size, so the total
     * estimate is well above the 10 KB breaker limit. The first render therefore trips; every
     * subsequent slot is filled via the {@code renderCbe} fast-path without issuing any searches.
     */
    public void testLargeMsearchTemplateDoesNotOom() throws Exception {
        createIndex("large-msearch");

        // Build a ~16 KB rendered source (2 000 stored_fields entries, no template variables).
        // stored_fields is a plain string array that SearchSourceBuilder accepts without any
        // named-XContent extensions, so it round-trips cleanly through render → parse → execute.
        StringBuilder sb = new StringBuilder("{\"size\":0,\"stored_fields\":[");
        for (int j = 0; j < 2_000; j++) {
            if (j > 0) sb.append(",");
            sb.append("\"f").append(j).append("\"");
        }
        sb.append("]}");
        String largeTemplate = sb.toString();

        // Tighten the REQUEST breaker to 10 KB. The render estimate for the first item (~32 KB)
        // already exceeds this limit, so the breaker trips immediately and every slot is a CBE.
        updateClusterSettings(
            Settings.builder().put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10kb")
        );
        try {
            int numRequests = 20;
            MultiSearchTemplateRequest multiRequest = new MultiSearchTemplateRequest();
            for (int i = 0; i < numRequests; i++) {
                SearchTemplateRequest req = new SearchTemplateRequest();
                req.setRequest(new SearchRequest("large-msearch"));
                req.setScriptType(ScriptType.INLINE);
                req.setScript(largeTemplate);
                multiRequest.add(req);
            }

            assertResponse(client().execute(MustachePlugin.MULTI_SEARCH_TEMPLATE_ACTION, multiRequest), response -> {
                assertThat(response.getResponses().length, equalTo(numRequests));
                // Once the first render trips the breaker, fillRemainingWithCbe fills every
                // subsequent slot via the renderCbe fast-path. All slots must be CBE failures —
                // a weaker "cbeCount > 0" check would miss a regression where only the first
                // slot is a CBE and the rest execute as real searches.
                for (Item item : response.getResponses()) {
                    assertNotNull("every slot must be populated", item);
                    assertTrue("every slot must be a CBE failure", item.isFailure());
                    assertThat(item.getFailure(), instanceOf(CircuitBreakingException.class));
                }
            });
        } finally {
            updateClusterSettings(
                Settings.builder().putNull(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey())
            );
        }
    }

    /**
     * Verifies that the {@code X-Elasticsearch-Search-Metrics} header is absent for a simulate-only
     * {@code _msearch/template} request (no disk reads occur, so {@link MultiSearchTemplateResponse#mergeDirectoryMetrics()}
     * returns empty metrics and the header must not be emitted).
     *
     * <p>When directory metrics are enabled (via {@link Store#DIRECTORY_METRICS_FEATURE_FLAG}), the header
     * is also asserted present for a real search, confirming end-to-end wiring of
     * {@code mergeDirectoryMetrics} through {@code wrapWithSearchMetricsHeader} in the REST action.
     */
    public void testSearchMetricsResponseHeader() throws Exception {
        createIndex("hdr-test");
        prepareIndex("hdr-test").setId("1").setSource("field", "value").get();
        refresh("hdr-test");

        // Simulate-only: no disk reads occur, so the header must always be absent.
        Request simulateReq = new Request("POST", "/_msearch/template");
        simulateReq.setJsonEntity(
            "{\"index\":\"hdr-test\"}\n" + "{\"source\":\"{\\\"query\\\":{\\\"match_all\\\":{}}}\",\"params\":{},\"simulate\":true}\n"
        );
        Response simulateResp = getRestClient().performRequest(simulateReq);
        long simulateHeaderCount = Arrays.stream(simulateResp.getHeaders())
            .filter(h -> h.getName().equalsIgnoreCase(RestActions.SEARCH_METRICS_RESPONSE_HEADER))
            .count();
        assertThat("simulate-only request must not emit the search-metrics header", simulateHeaderCount, equalTo(0L));

        // Real search: header present only when the feature flag is enabled and file reads occur.
        assumeTrue("directory metrics feature flag must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
        Request realReq = new Request("POST", "/_msearch/template");
        realReq.setJsonEntity("{\"index\":\"hdr-test\"}\n" + "{\"source\":\"{\\\"query\\\":{\\\"match_all\\\":{}}}\",\"params\":{}}\n");
        Response realResp = getRestClient().performRequest(realReq);
        long realHeaderCount = Arrays.stream(realResp.getHeaders())
            .filter(h -> h.getName().equalsIgnoreCase(RestActions.SEARCH_METRICS_RESPONSE_HEADER))
            .count();
        assertThat("real msearch/template must emit exactly one consolidated search-metrics header", realHeaderCount, equalTo(1L));
    }

    /**
     * Verifies that cancelling the outer {@code _msearch/template} task propagates to the inner
     * {@code _msearch} searches: after cancellation all slots are failures rather than leaving the
     * outer listener hanging indefinitely.
     *
     * <p>A blocking script holds each shard search open until the outer task is cancelled and the
     * block is released. The response must be a {@link MultiSearchTemplateResponse} (not a top-level
     * exception), and every item must be a failure.
     */
    public void testCancellationPropagatesFromParentToInnerSearches() throws Exception {
        createIndex("cancel-test");
        for (int i = 0; i < 5; i++) {
            prepareIndex("cancel-test").setId(Integer.toString(i)).setSource("field", "value").get();
        }
        refresh("cancel-test");

        List<AbstractSearchCancellationTestCase.ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService ps : internalCluster().getInstances(PluginsService.class)) {
            ps.filterPlugins(AbstractSearchCancellationTestCase.ScriptedBlockPlugin.class).forEach(p -> {
                p.reset();
                p.enableBlock();
                plugins.add(p);
            });
        }

        String blockingTemplate = "{\"query\":{\"script\":{\"script\":{\"source\":\""
            + AbstractSearchCancellationTestCase.ScriptedBlockPlugin.SEARCH_BLOCK_SCRIPT_NAME
            + "\",\"lang\":\"mockscript\"}}}}";

        MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
        SearchTemplateRequest str = new SearchTemplateRequest();
        str.setRequest(new SearchRequest("cancel-test"));
        str.setScriptType(ScriptType.INLINE);
        str.setScript(blockingTemplate);
        request.add(str);

        ActionFuture<MultiSearchTemplateResponse> future = client().execute(MustachePlugin.MULTI_SEARCH_TEMPLATE_ACTION, request);

        // Use setBeforeExecution to latch on the first shard hit, since hits is package-private.
        java.util.concurrent.CountDownLatch hitLatch = new java.util.concurrent.CountDownLatch(1);
        for (AbstractSearchCancellationTestCase.ScriptedBlockPlugin plugin : plugins) {
            plugin.setBeforeExecution(hitLatch::countDown);
        }

        // Wait until at least one shard has entered the blocking script.
        assertTrue("timed out waiting for shard to be blocked", hitLatch.await(10, TimeUnit.SECONDS));

        // Cancel the outer _msearch/template task — propagates to inner searches via parent-task linkage.
        ListTasksResponse listResp = clusterAdmin().prepareListTasks().setActions(MustachePlugin.MULTI_SEARCH_TEMPLATE_ACTION.name()).get();
        assertThat("outer msearch/template task must be present", listResp.getTasks(), hasSize(1));
        TaskInfo outerTask = listResp.getTasks().get(0);
        clusterAdmin().prepareCancelTasks().setTargetTaskId(outerTask.taskId()).get();

        // Unblock the shard-level scripts so the search threads can complete.
        for (AbstractSearchCancellationTestCase.ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }

        // The outer response must complete (not hang) and all items must be failures.
        MultiSearchTemplateResponse response = future.actionGet();
        try {
            assertThat(response.getResponses().length, equalTo(1));
            assertTrue("cancelled item must be a failure", response.getResponses()[0].isFailure());
        } finally {
            response.decRef();
        }
    }

    /**
    * Test that triggering the CCS compatibility check with a query that shouldn't go to the minor before TransportVersion.current() works
    */
    public void testCCSCheckCompatibility() throws Exception {
        String templateString = """
            {
            "source": "{ \\"query\\":{\\"fail_before_current_version\\":{}} }"
            }""";
        SearchTemplateRequest searchTemplateRequest = SearchTemplateRequest.fromXContent(
            createParser(JsonXContent.jsonXContent, templateString)
        );
        searchTemplateRequest.setRequest(new SearchRequest());
        MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
        request.add(searchTemplateRequest);
        assertResponse(client().execute(MustachePlugin.MULTI_SEARCH_TEMPLATE_ACTION, request), multiSearchTemplateResponse -> {
            Item response = multiSearchTemplateResponse.getResponses()[0];
            assertTrue(response.isFailure());
            Exception ex = response.getFailure();
            assertThat(
                ex.getMessage(),
                containsString("[class org.elasticsearch.action.search.SearchRequest] is not compatible with version")
            );
            assertThat(ex.getMessage(), containsString("'search.check_ccs_compatibility' setting is enabled."));

            String expectedCause = Strings.format(
                "[fail_before_current_version] was released first in version %s, failed compatibility "
                    + "check trying to send it to node with version %s",
                FailBeforeCurrentVersionQueryBuilder.FUTURE_VERSION.toReleaseVersion(),
                TransportVersion.minimumCCSVersion().toReleaseVersion()
            );
            String actualCause = ex.getCause().getMessage();
            assertEquals(expectedCause, actualCause);
        });
    }
}
