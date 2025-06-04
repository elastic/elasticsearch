/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Strings;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
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
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.NodeRoles.ingestOnlyNode;
import static org.elasticsearch.test.NodeRoles.masterOnlyNode;
import static org.elasticsearch.test.NodeRoles.nonIngestNode;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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
            .put("thread_pool.write.size", 2)
            .build();
    }

    public void testEnrichAPIs() throws ExecutionException, InterruptedException {
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
            PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName, enrichPolicy);
            client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
            client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName))
                .actionGet();

            EnrichPolicy.NamedPolicy result = client().execute(
                GetEnrichPolicyAction.INSTANCE,
                new GetEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName)
            ).actionGet().getPolicies().get(0);
            assertThat(result, equalTo(new EnrichPolicy.NamedPolicy(policyName, enrichPolicy)));
            String enrichIndexPrefix = EnrichPolicy.getBaseName(policyName) + "*";
            refresh(enrichIndexPrefix);
            assertHitCount(client().search(new SearchRequest(enrichIndexPrefix)), numDocsInSourceIndex);
        }

        GetEnrichPolicyAction.Response response = client().execute(
            GetEnrichPolicyAction.INSTANCE,
            new GetEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT)
        ).actionGet();
        assertThat(response.getPolicies().size(), equalTo(numPolicies));

        for (int i = 0; i < numPolicies; i++) {
            String policyName = POLICY_NAME + i;
            client().execute(DeleteEnrichPolicyAction.INSTANCE, new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName))
                .actionGet();
        }

        response = client().execute(GetEnrichPolicyAction.INSTANCE, new GetEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT)).actionGet();
        assertThat(response.getPolicies().size(), equalTo(0));
    }

    public void testEnrich() {
        List<String> nodes = internalCluster().startNodes(3);
        List<String> keys = createSourceIndex(64);
        createAndExecutePolicy();
        createPipeline();
        enrich(keys, randomFrom(nodes));
    }

    public void testStressEnrich() {
        List<String> nodes = internalCluster().startNodes(
            3,
            Settings.builder().put("enrich.coordinator_proxy.max_concurrent_requests", 1).build()
        );
        int indices = randomIntBetween(5, 10);
        final Map<String, List<String>> keys = Maps.newHashMapWithExpectedSize(indices);
        for (int i = 0; i < indices; i++) {
            final String indexName = "index-" + i;
            List<String> k = createSourceIndex(indexName, 64);
            final String policyName = "policy-" + i;
            createAndExecutePolicy(policyName, indexName);
            final String pipelineName = "pipeline-" + i;
            createPipeline(policyName, pipelineName);
            keys.put(pipelineName, k);
        }
        enrich(keys, randomFrom(nodes), 50);
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

    public void testExecutePolicyWithDedicatedMasterNodes() throws Exception {
        var masterNodes = internalCluster().startNodes(3, masterOnlyNode());
        var regularNodes = internalCluster().startNodes(2, nonMasterNode());
        ensureStableCluster(5, (String) null);

        assertAcked(prepareCreate(SOURCE_INDEX_NAME).setMapping(MATCH_FIELD, "type=keyword"));
        var enrichPolicy = new EnrichPolicy(
            EnrichPolicy.MATCH_TYPE,
            null,
            List.of(SOURCE_INDEX_NAME),
            MATCH_FIELD,
            List.of(DECORATE_FIELDS)
        );
        var putPolicyRequest = new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, POLICY_NAME, enrichPolicy);
        assertAcked(client().execute(PutEnrichPolicyAction.INSTANCE, putPolicyRequest).actionGet());
        var executePolicyRequest = new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, POLICY_NAME);
        executePolicyRequest.setWaitForCompletion(false); // From tne returned taks id the node that executes the policy can be determined
        var executePolicyResponse = client().execute(ExecuteEnrichPolicyAction.INSTANCE, executePolicyRequest).actionGet();
        assertThat(executePolicyResponse.getStatus(), nullValue());
        assertThat(executePolicyResponse.getTaskId(), notNullValue());

        var getTaskRequest = new GetTaskRequest().setTaskId(executePolicyResponse.getTaskId()).setWaitForCompletion(true);
        clusterAdmin().getTask(getTaskRequest).actionGet();

        var discoNodes = clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)).actionGet().getState().nodes();
        assertThat(discoNodes.get(executePolicyResponse.getTaskId().getNodeId()).isMasterNode(), is(false));
    }

    public void testExecutePolicyNeverOnElectedMaster() throws Exception {
        internalCluster().startNodes(3);
        ensureStableCluster(3, (String) null);

        assertAcked(prepareCreate(SOURCE_INDEX_NAME).setMapping(MATCH_FIELD, "type=keyword"));
        var enrichPolicy = new EnrichPolicy(
            EnrichPolicy.MATCH_TYPE,
            null,
            List.of(SOURCE_INDEX_NAME),
            MATCH_FIELD,
            List.of(DECORATE_FIELDS)
        );
        var putPolicyRequest = new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, POLICY_NAME, enrichPolicy);
        assertAcked(client().execute(PutEnrichPolicyAction.INSTANCE, putPolicyRequest).actionGet());
        var executePolicyRequest = new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, POLICY_NAME);
        executePolicyRequest.setWaitForCompletion(false); // From tne returned taks id the node that executes the policy can be determined
        var executePolicyResponse = client().execute(ExecuteEnrichPolicyAction.INSTANCE, executePolicyRequest).actionGet();
        assertThat(executePolicyResponse.getStatus(), nullValue());
        assertThat(executePolicyResponse.getTaskId(), notNullValue());

        var getTaskRequest = new GetTaskRequest().setTaskId(executePolicyResponse.getTaskId()).setWaitForCompletion(true);
        clusterAdmin().getTask(getTaskRequest).actionGet();

        var discoNodes = clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)).actionGet().getState().nodes();
        assertThat(executePolicyResponse.getTaskId().getNodeId(), not(equalTo(discoNodes.getMasterNodeId())));
    }

    private static void enrich(List<String> keys, String coordinatingNode) {
        enrich(Map.of(PIPELINE_NAME, keys), coordinatingNode, 256);
    }

    private static void enrich(Map<String, List<String>> keys, String coordinatingNode, int numDocs) {
        final String[] executedPipeline = new String[2 * numDocs];
        BulkRequest bulkRequest = new BulkRequest("my-index");
        for (int i = 0; i < numDocs; i++) {
            final String pipeline = randomFrom(keys.keySet());
            executedPipeline[i] = pipeline;
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.id(Integer.toString(i));
            indexRequest.setPipeline(pipeline);
            indexRequest.source(Map.of(MATCH_FIELD, randomFrom(keys.get(pipeline))));
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
            assertThat(keys.get(executedPipeline[i]), containsInRelativeOrder(userEntry.get(MATCH_FIELD)));
            for (String field : DECORATE_FIELDS) {
                assertThat(userEntry.get(field), notNullValue());
            }
        }

        EnrichStatsAction.Response statsResponse = client().execute(
            EnrichStatsAction.INSTANCE,
            new EnrichStatsAction.Request(TEST_REQUEST_TIMEOUT)
        ).actionGet();
        assertThat(statsResponse.getCoordinatorStats().size(), equalTo(internalCluster().size()));
        String nodeId = getNodeId(coordinatingNode);
        CoordinatorStats stats = statsResponse.getCoordinatorStats().stream().filter(s -> s.nodeId().equals(nodeId)).findAny().get();
        assertThat(stats.nodeId(), equalTo(nodeId));
        assertThat(stats.remoteRequestsTotal(), greaterThanOrEqualTo(1L));
        // 'numDocs' lookups are done, but not 'numDocs' searches, because searches may get cached:
        // and not all enrichments may happen via the same node.
        assertThat(stats.executedSearchesTotal(), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo((long) numDocs)));
    }

    private static List<String> createSourceIndex(int numDocs) {
        return createSourceIndex(SOURCE_INDEX_NAME, numDocs);
    }

    private static List<String> createSourceIndex(String indexName, int numDocs) {
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < numDocs; i++) {
            String key;
            do {
                key = randomAlphaOfLength(16);
            } while (keys.add(key) == false);

            IndexRequest indexRequest = new IndexRequest(indexName);
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
        indicesAdmin().refresh(new RefreshRequest(indexName)).actionGet();
        return List.copyOf(keys);
    }

    private static void createAndExecutePolicy() {
        createAndExecutePolicy(POLICY_NAME, SOURCE_INDEX_NAME);
    }

    private static void createAndExecutePolicy(String policyName, String indexName) {
        EnrichPolicy enrichPolicy = new EnrichPolicy(
            EnrichPolicy.MATCH_TYPE,
            null,
            List.of(indexName),
            MATCH_FIELD,
            List.of(DECORATE_FIELDS)
        );
        PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName, enrichPolicy);
        client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
        final ActionFuture<ExecuteEnrichPolicyAction.Response> policyExecuteFuture = client().execute(
            ExecuteEnrichPolicyAction.INSTANCE,
            new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName)
        );
        // Make sure we can deserialize enrich policy execution task status
        final List<TaskInfo> tasks = clusterAdmin().prepareListTasks().setActions(EnrichPolicyExecutor.TASK_ACTION).get().getTasks();
        // Best effort, sometimes the enrich policy task will not be visible yet or will have already finished
        if (tasks.isEmpty() == false) {
            try {
                final GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(tasks.get(0).taskId()).get();
                assertEquals(getTaskResponse.getTask().getTask().action(), EnrichPolicyExecutor.TASK_ACTION);
            } catch (ResourceNotFoundException e) {
                // ignored, could be the task has already finished
            }
        }
        policyExecuteFuture.actionGet();
    }

    private static void createPipeline() {
        createPipeline(POLICY_NAME, PIPELINE_NAME);
    }

    private static void createPipeline(String policyName, String pipelineName) {
        putJsonPipeline(pipelineName, Strings.format("""
            {
              "processors": [ { "enrich": { "policy_name": "%s", "field": "%s", "target_field": "user" } } ]
            }""", policyName, MATCH_FIELD));
    }
}
