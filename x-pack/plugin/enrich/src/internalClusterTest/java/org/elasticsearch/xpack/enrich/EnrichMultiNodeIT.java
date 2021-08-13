/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.NodeRoles.ingestOnlyNode;
import static org.elasticsearch.test.NodeRoles.nonIngestNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class EnrichMultiNodeIT extends ESIntegTestCase {

    static final String POLICY_NAME = "my-policy";
    private static final String PIPELINE_NAME = "my-pipeline";
    static final String SOURCE_INDEX_NAME = "users";
    static final String MATCH_FIELD = "email";
    static final String[] DECORATE_FIELDS = new String[] { "address", "city", "country" };

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateEnrich.class, ReindexPlugin.class, IngestCommonPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // TODO Change this to run with security enabled
            // https://github.com/elastic/elasticsearch/issues/75940
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .build();
    }

    public void testEnrichAPIs() {
        final int numPolicies = randomIntBetween(2, 4);
        internalCluster().startNodes(randomIntBetween(2, 3));
        int numDocsInSourceIndex = randomIntBetween(8, 32);
        createSourceIndex(numDocsInSourceIndex);

        for (int i = 0; i < numPolicies; i++) {
            String policyName = POLICY_NAME + i;
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

            EnrichPolicy.NamedPolicy result = client().execute(
                GetEnrichPolicyAction.INSTANCE,
                new GetEnrichPolicyAction.Request(new String[] { policyName })
            ).actionGet().getPolicies().get(0);
            assertThat(result, equalTo(new EnrichPolicy.NamedPolicy(policyName, enrichPolicy)));
            String enrichIndexPrefix = EnrichPolicy.getBaseName(policyName) + "*";
            refresh(enrichIndexPrefix);
            SearchResponse searchResponse = client().search(new SearchRequest(enrichIndexPrefix)).actionGet();
            assertThat(searchResponse.getHits().getTotalHits().relation, equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) numDocsInSourceIndex));
        }

        GetEnrichPolicyAction.Response response = client().execute(GetEnrichPolicyAction.INSTANCE, new GetEnrichPolicyAction.Request())
            .actionGet();
        assertThat(response.getPolicies().size(), equalTo(numPolicies));

        for (int i = 0; i < numPolicies; i++) {
            String policyName = POLICY_NAME + i;
            client().execute(DeleteEnrichPolicyAction.INSTANCE, new DeleteEnrichPolicyAction.Request(policyName)).actionGet();
        }

        response = client().execute(GetEnrichPolicyAction.INSTANCE, new GetEnrichPolicyAction.Request()).actionGet();
        assertThat(response.getPolicies().size(), equalTo(0));
    }

    public void testEnrich() {
        List<String> nodes = internalCluster().startNodes(3);
        List<String> keys = createSourceIndex(64);
        createAndExecutePolicy();
        createPipeline();
        enrich(keys, randomFrom(nodes));
    }

    public void testEnrichDedicatedIngestNode() {
        internalCluster().startNode();
        String ingestOnlyNode = internalCluster().startNode(ingestOnlyNode());

        List<String> keys = createSourceIndex(64);
        createAndExecutePolicy();
        createPipeline();
        enrich(keys, ingestOnlyNode);
    }

    public void testEnrichNoIngestNodes() {
        Settings settings = Settings.builder().put(nonIngestNode()).build();
        internalCluster().startNode(settings);

        createSourceIndex(64);
        Exception e = expectThrows(IllegalStateException.class, EnrichMultiNodeIT::createAndExecutePolicy);
        assertThat(e.getMessage(), equalTo("no ingest nodes in this cluster"));
    }

    private static void enrich(List<String> keys, String coordinatingNode) {
        int numDocs = 256;
        BulkRequest bulkRequest = new BulkRequest("my-index");
        for (int i = 0; i < numDocs; i++) {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.id(Integer.toString(i));
            indexRequest.setPipeline(PIPELINE_NAME);
            indexRequest.source(Map.of(MATCH_FIELD, randomFrom(keys)));
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertThat("Expected no failure, but " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures(), is(false));
        int expectedId = 0;
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getId(), equalTo(Integer.toString(expectedId++)));
        }

        for (int i = 0; i < numDocs; i++) {
            GetResponse getResponse = client().get(new GetRequest("my-index", Integer.toString(i))).actionGet();
            Map<String, Object> source = getResponse.getSourceAsMap();
            Map<?, ?> userEntry = (Map<?, ?>) source.get("user");
            assertThat(userEntry.size(), equalTo(DECORATE_FIELDS.length + 1));
            assertThat(keys.contains(userEntry.get(MATCH_FIELD)), is(true));
            for (String field : DECORATE_FIELDS) {
                assertThat(userEntry.get(field), notNullValue());
            }
        }

        EnrichStatsAction.Response statsResponse = client().execute(EnrichStatsAction.INSTANCE, new EnrichStatsAction.Request())
            .actionGet();
        assertThat(statsResponse.getCoordinatorStats().size(), equalTo(internalCluster().size()));
        String nodeId = internalCluster().getInstance(ClusterService.class, coordinatingNode).localNode().getId();
        CoordinatorStats stats = statsResponse.getCoordinatorStats().stream().filter(s -> s.getNodeId().equals(nodeId)).findAny().get();
        assertThat(stats.getNodeId(), equalTo(nodeId));
        assertThat(stats.getRemoteRequestsTotal(), greaterThanOrEqualTo(1L));
        assertThat(stats.getExecutedSearchesTotal(), equalTo((long) numDocs));
    }

    private static List<String> createSourceIndex(int numDocs) {
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < numDocs; i++) {
            String key;
            do {
                key = randomAlphaOfLength(16);
            } while (keys.add(key) == false);

            IndexRequest indexRequest = new IndexRequest(SOURCE_INDEX_NAME);
            indexRequest.create(true);
            indexRequest.id(key);
            indexRequest.source(
                Map.of(
                    MATCH_FIELD,
                    key,
                    DECORATE_FIELDS[0],
                    randomAlphaOfLength(4),
                    DECORATE_FIELDS[1],
                    randomAlphaOfLength(4),
                    DECORATE_FIELDS[2],
                    randomAlphaOfLength(4)
                )
            );
            client().index(indexRequest).actionGet();
        }
        client().admin().indices().refresh(new RefreshRequest(SOURCE_INDEX_NAME)).actionGet();
        return List.copyOf(keys);
    }

    private static void createAndExecutePolicy() {
        EnrichPolicy enrichPolicy = new EnrichPolicy(
            EnrichPolicy.MATCH_TYPE,
            null,
            List.of(SOURCE_INDEX_NAME),
            MATCH_FIELD,
            List.of(DECORATE_FIELDS)
        );
        PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(POLICY_NAME, enrichPolicy);
        client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
        final ActionFuture<ExecuteEnrichPolicyAction.Response> policyExecuteFuture = client().execute(
            ExecuteEnrichPolicyAction.INSTANCE,
            new ExecuteEnrichPolicyAction.Request(POLICY_NAME)
        );
        // Make sure we can deserialize enrich policy execution task status
        final List<TaskInfo> tasks = client().admin()
            .cluster()
            .prepareListTasks()
            .setActions(EnrichPolicyExecutor.TASK_ACTION)
            .get()
            .getTasks();
        // Best effort, sometimes the enrich policy task will not be visible yet or will have already finished
        if (tasks.isEmpty() == false) {
            try {
                final GetTaskResponse getTaskResponse = client().admin().cluster().prepareGetTask(tasks.get(0).getTaskId()).get();
                assertEquals(getTaskResponse.getTask().getTask().getAction(), EnrichPolicyExecutor.TASK_ACTION);
            } catch (ResourceNotFoundException e) {
                // ignored, could be the task has already finished
            }
        }
        policyExecuteFuture.actionGet();
    }

    private static void createPipeline() {
        String pipelineBody = "{\"processors\": [{\"enrich\": {\"policy_name\":\""
            + POLICY_NAME
            + "\", \"field\": \""
            + MATCH_FIELD
            + "\", \"target_field\": \"user\"}}]}";
        PutPipelineRequest request = new PutPipelineRequest(PIPELINE_NAME, new BytesArray(pipelineBody), XContentType.JSON);
        client().admin().cluster().putPipeline(request).actionGet();
    }
}
