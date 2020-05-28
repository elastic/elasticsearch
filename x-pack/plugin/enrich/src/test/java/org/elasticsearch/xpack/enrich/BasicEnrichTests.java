/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geo.GeoPlugin;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class BasicEnrichTests extends ESSingleNodeTestCase {

    static final String SOURCE_INDEX_NAME = "users";
    static final String MATCH_FIELD = "email";
    static final String[] DECORATE_FIELDS = new String[] { "address", "city", "country" };

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateEnrich.class, ReindexPlugin.class, IngestCommonPlugin.class, MustachePlugin.class, GeoPlugin.class);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testIngestDataWithMatchProcessor() {
        int numDocs = 32;
        int maxMatches = randomIntBetween(2, 8);
        List<String> keys = createSourceMatchIndex(numDocs, maxMatches);

        String policyName = "my-policy";
        EnrichPolicy enrichPolicy = new EnrichPolicy(
            EnrichPolicy.MATCH_TYPE,
            null,
            List.of(SOURCE_INDEX_NAME),
            MATCH_FIELD,
            List.of(DECORATE_FIELDS)
        );
        PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(policyName, enrichPolicy);
        client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
        client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(policyName)).actionGet();

        String pipelineName = "my-pipeline";
        String pipelineBody = "{\"processors\": [{\"enrich\": {\"policy_name\":\""
            + policyName
            + "\", \"field\": \""
            + MATCH_FIELD
            + "\", \"target_field\": \"users\", \"max_matches\": "
            + maxMatches
            + "}}]}";
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest(pipelineName, new BytesArray(pipelineBody), XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).actionGet();

        BulkRequest bulkRequest = new BulkRequest("my-index");
        for (int i = 0; i < numDocs; i++) {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.id(Integer.toString(i));
            indexRequest.setPipeline(pipelineName);
            indexRequest.source(Map.of(MATCH_FIELD, keys.get(i)));
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat("Expected no failure, but " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures(), is(false));
        int expectedId = 0;
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getId(), equalTo(Integer.toString(expectedId++)));
        }

        for (int doc = 0; doc < numDocs; doc++) {
            GetResponse getResponse = client().get(new GetRequest("my-index", Integer.toString(doc))).actionGet();
            Map<String, Object> source = getResponse.getSourceAsMap();
            List<?> userEntries = (List<?>) source.get("users");
            assertThat(userEntries, notNullValue());
            assertThat(userEntries.size(), equalTo(maxMatches));
            for (int i = 0; i < maxMatches; i++) {
                Map<?, ?> userEntry = (Map<?, ?>) userEntries.get(i);
                assertThat(userEntry.size(), equalTo(DECORATE_FIELDS.length + 1));
                for (int j = 0; j < 3; j++) {
                    String field = DECORATE_FIELDS[j];
                    assertThat(userEntry.get(field), equalTo(keys.get(doc) + j));
                }
                assertThat(keys.contains(userEntry.get(MATCH_FIELD)), is(true));
            }
        }

        EnrichStatsAction.Response statsResponse = client().execute(EnrichStatsAction.INSTANCE, new EnrichStatsAction.Request())
            .actionGet();
        assertThat(statsResponse.getCoordinatorStats().size(), equalTo(1));
        String localNodeId = getInstanceFromNode(ClusterService.class).localNode().getId();
        assertThat(statsResponse.getCoordinatorStats().get(0).getNodeId(), equalTo(localNodeId));
        assertThat(statsResponse.getCoordinatorStats().get(0).getRemoteRequestsTotal(), greaterThanOrEqualTo(1L));
        assertThat(statsResponse.getCoordinatorStats().get(0).getExecutedSearchesTotal(), equalTo((long) numDocs));
    }

    public void testIngestDataWithGeoMatchProcessor() {
        String matchField = "location";
        String enrichField = "zipcode";
        // create enrich index
        {
            IndexRequest indexRequest = new IndexRequest(SOURCE_INDEX_NAME);
            indexRequest.source(
                Map.of(
                    matchField,
                    "POLYGON(("
                        + "-122.08592534065245 37.38501746624134,"
                        + "-122.08193421363829 37.38501746624134,"
                        + "-122.08193421363829 37.3879329075567,"
                        + "-122.08592534065245 37.3879329075567,"
                        + "-122.08592534065245 37.38501746624134))",
                    "zipcode",
                    "94040"
                )
            );
            client().index(indexRequest).actionGet();
            client().admin().indices().refresh(new RefreshRequest(SOURCE_INDEX_NAME)).actionGet();
        }

        String policyName = "my-policy";
        EnrichPolicy enrichPolicy = new EnrichPolicy(
            EnrichPolicy.GEO_MATCH_TYPE,
            null,
            List.of(SOURCE_INDEX_NAME),
            matchField,
            List.of(enrichField)
        );
        PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(policyName, enrichPolicy);
        client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
        client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(policyName)).actionGet();

        String pipelineName = "my-pipeline";
        String pipelineBody = "{\"processors\": [{\"enrich\": {\"policy_name\":\""
            + policyName
            + "\", \"field\": \""
            + matchField
            + "\", \"target_field\": \"enriched\", \"max_matches\": 1 }}]}";
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest(pipelineName, new BytesArray(pipelineBody), XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).actionGet();

        BulkRequest bulkRequest = new BulkRequest("my-index");
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.id("_id");
        indexRequest.setPipeline(pipelineName);
        indexRequest.source(Map.of(matchField, "37.386444, -122.083863")); // point within match boundary
        bulkRequest.add(indexRequest);
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat("Expected no failure, but " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures(), is(false));
        assertThat(bulkResponse.getItems().length, equalTo(1));
        assertThat(bulkResponse.getItems()[0].getId(), equalTo("_id"));

        GetResponse getResponse = client().get(new GetRequest("my-index", "_id")).actionGet();
        Map<String, Object> source = getResponse.getSourceAsMap();
        Map<?, ?> entries = (Map) source.get("enriched");
        assertThat(entries, notNullValue());
        assertThat(entries.size(), equalTo(2));
        assertThat(entries.containsKey(matchField), is(true));
        assertThat(entries.get(enrichField), equalTo("94040"));

        EnrichStatsAction.Response statsResponse = client().execute(EnrichStatsAction.INSTANCE, new EnrichStatsAction.Request())
            .actionGet();
        assertThat(statsResponse.getCoordinatorStats().size(), equalTo(1));
        String localNodeId = getInstanceFromNode(ClusterService.class).localNode().getId();
        assertThat(statsResponse.getCoordinatorStats().get(0).getNodeId(), equalTo(localNodeId));
        assertThat(statsResponse.getCoordinatorStats().get(0).getRemoteRequestsTotal(), greaterThanOrEqualTo(1L));
        assertThat(statsResponse.getCoordinatorStats().get(0).getExecutedSearchesTotal(), equalTo(1L));
    }

    public void testMultiplePolicies() {
        int numPolicies = 8;
        for (int i = 0; i < numPolicies; i++) {
            String policyName = "policy" + i;

            IndexRequest indexRequest = new IndexRequest("source-" + i);
            indexRequest.source("key", "key", "value", "val" + i);
            client().index(indexRequest).actionGet();
            client().admin().indices().refresh(new RefreshRequest("source-" + i)).actionGet();

            EnrichPolicy enrichPolicy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("source-" + i), "key", List.of("value"));
            PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(policyName, enrichPolicy);
            client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
            client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(policyName)).actionGet();

            String pipelineName = "pipeline" + i;
            String pipelineBody = "{\"processors\": [{\"enrich\": {\"policy_name\":\""
                + policyName
                + "\", \"field\": \"key\", \"target_field\": \"target\"}}]}";
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest(pipelineName, new BytesArray(pipelineBody), XContentType.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).actionGet();
        }

        BulkRequest bulkRequest = new BulkRequest("my-index");
        for (int i = 0; i < numPolicies; i++) {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.id(Integer.toString(i));
            indexRequest.setPipeline("pipeline" + i);
            indexRequest.source(Map.of("key", "key"));
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat("Expected no failure, but " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures(), is(false));

        for (int i = 0; i < numPolicies; i++) {
            GetResponse getResponse = client().get(new GetRequest("my-index", Integer.toString(i))).actionGet();
            Map<String, Object> source = getResponse.getSourceAsMap();
            assertThat(source.size(), equalTo(2));
            assertThat(source.get("target"), equalTo(Map.of("key", "key", "value", "val" + i)));
        }
    }

    public void testAsyncTaskExecute() throws Exception {
        String policyName = "async-policy";
        String sourceIndexName = "async-policy-source";

        {
            IndexRequest indexRequest = new IndexRequest(sourceIndexName);
            indexRequest.source("key", "key", "value", "val1");
            client().index(indexRequest).actionGet();
            client().admin().indices().refresh(new RefreshRequest(sourceIndexName)).actionGet();
        }

        EnrichPolicy enrichPolicy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndexName), "key", List.of("value"));
        PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(policyName, enrichPolicy);
        client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
        ExecuteEnrichPolicyAction.Response executeResponse = client().execute(
            ExecuteEnrichPolicyAction.INSTANCE,
            new ExecuteEnrichPolicyAction.Request(policyName).setWaitForCompletion(false)
        ).actionGet();

        assertThat(executeResponse.getStatus(), is(nullValue()));
        assertThat(executeResponse.getTaskId(), is(not(nullValue())));
        GetTaskRequest getPolicyTaskRequest = new GetTaskRequest().setTaskId(executeResponse.getTaskId()).setWaitForCompletion(true);
        assertBusy(() -> {
            GetTaskResponse taskResponse = client().execute(GetTaskAction.INSTANCE, getPolicyTaskRequest).actionGet();
            assertThat(
                ((ExecuteEnrichPolicyStatus) taskResponse.getTask().getTask().getStatus()).getPhase(),
                is(ExecuteEnrichPolicyStatus.PolicyPhases.COMPLETE)
            );
        });

        String pipelineName = "test-pipeline";
        String pipelineBody = "{\"processors\": [{\"enrich\": {\"policy_name\":\""
            + policyName
            + "\", \"field\": \"key\", \"target_field\": \"target\"}}]}";
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest(pipelineName, new BytesArray(pipelineBody), XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).actionGet();

        BulkRequest bulkRequest = new BulkRequest("my-index");
        int numTestDocs = randomIntBetween(3, 10);
        for (int i = 0; i < numTestDocs; i++) {
            IndexRequest indexRequest = new IndexRequest("my-index");
            indexRequest.id(Integer.toString(i));
            indexRequest.setPipeline(pipelineName);
            indexRequest.source(Map.of("key", "key"));
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat("Expected no failure, but " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures(), is(false));

        for (int i = 0; i < numTestDocs; i++) {
            GetResponse getResponse = client().get(new GetRequest("my-index", Integer.toString(i))).actionGet();
            Map<String, Object> source = getResponse.getSourceAsMap();
            assertThat(source.size(), equalTo(2));
            assertThat(source.get("target"), equalTo(Map.of("key", "key", "value", "val1")));
        }
    }

    public void testTemplating() throws Exception {
        List<String> keys = createSourceMatchIndex(1, 1);
        String policyName = "my-policy";
        EnrichPolicy enrichPolicy = new EnrichPolicy(
            EnrichPolicy.MATCH_TYPE,
            null,
            List.of(SOURCE_INDEX_NAME),
            MATCH_FIELD,
            List.of(DECORATE_FIELDS)
        );
        PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(policyName, enrichPolicy);
        client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
        client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(policyName)).actionGet();

        String pipelineName = "my-pipeline";
        String pipelineBody = "{\"processors\": [{\"enrich\": {\"policy_name\":\""
            + policyName
            + "\", \"field\": \"{{indirection1}}\", \"target_field\": \"{{indirection2}}\""
            + "}}]}";
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest(pipelineName, new BytesArray(pipelineBody), XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).actionGet();

        IndexRequest indexRequest = new IndexRequest("my-index").id("1")
            .setPipeline(pipelineName)
            .source(Map.of("indirection1", MATCH_FIELD, "indirection2", "users", MATCH_FIELD, keys.get(0)));
        client().index(indexRequest).get();
        GetResponse getResponse = client().get(new GetRequest("my-index", "1")).actionGet();
        Map<String, Object> source = getResponse.getSourceAsMap();
        Map<?, ?> userEntry = (Map<?, ?>) source.get("users");
        assertThat(userEntry.size(), equalTo(DECORATE_FIELDS.length + 1));
        for (int j = 0; j < 3; j++) {
            String field = DECORATE_FIELDS[j];
            assertThat(userEntry.get(field), equalTo(keys.get(0) + j));
        }
        assertThat(keys.contains(userEntry.get(MATCH_FIELD)), is(true));
    }

    private List<String> createSourceMatchIndex(int numKeys, int numDocsPerKey) {
        Set<String> keys = new HashSet<>();
        for (int id = 0; id < numKeys; id++) {
            String key;
            do {
                key = randomAlphaOfLength(16);
            } while (keys.add(key) == false);

            for (int doc = 0; doc < numDocsPerKey; doc++) {
                IndexRequest indexRequest = new IndexRequest(SOURCE_INDEX_NAME);
                indexRequest.source(
                    Map.of(MATCH_FIELD, key, DECORATE_FIELDS[0], key + "0", DECORATE_FIELDS[1], key + "1", DECORATE_FIELDS[2], key + "2")
                );
                client().index(indexRequest).actionGet();
            }
        }
        client().admin().indices().refresh(new RefreshRequest(SOURCE_INDEX_NAME)).actionGet();
        return List.copyOf(keys);
    }
}
