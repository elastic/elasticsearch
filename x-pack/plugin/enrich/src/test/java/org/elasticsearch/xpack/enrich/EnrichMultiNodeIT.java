/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ListEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class EnrichMultiNodeIT extends ESIntegTestCase {

    static final String POLICY_NAME = "my-policy";
    private static final String PIPELINE_NAME = "my-pipeline";
    static final String SOURCE_INDEX_NAME = "users";
    static final String KEY_FIELD = "email";
    static final String[] DECORATE_FIELDS = new String[]{"address", "city", "country"};

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(EnrichPlugin.class, ReindexPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    public void testEnrichAPIs() {
        final int numPolicies = randomIntBetween(2, 4);
        internalCluster().startNodes(randomIntBetween(2, 3));
        int numDocsInSourceIndex = randomIntBetween(8, 32);
        createSourceIndex(numDocsInSourceIndex);

        for (int i = 0; i < numPolicies; i++) {
            String policyName = POLICY_NAME + i;
            EnrichPolicy enrichPolicy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null,
                Arrays.asList(SOURCE_INDEX_NAME), KEY_FIELD, Arrays.asList(DECORATE_FIELDS));
            PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(policyName, enrichPolicy);
            client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
            client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(policyName)).actionGet();

            EnrichPolicy result =
                client().execute(GetEnrichPolicyAction.INSTANCE, new GetEnrichPolicyAction.Request(policyName)).actionGet().getPolicy();
            assertThat(result, equalTo(enrichPolicy));
            String enrichIndexPrefix = EnrichPolicy.getBaseName(policyName) + "*";
            refresh(enrichIndexPrefix);
            SearchResponse searchResponse = client().search(new SearchRequest(enrichIndexPrefix)).actionGet();
            assertThat(searchResponse.getHits().getTotalHits().relation, equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) numDocsInSourceIndex));
        }

        ListEnrichPolicyAction.Response response =
            client().execute(ListEnrichPolicyAction.INSTANCE, new ListEnrichPolicyAction.Request()).actionGet();
        assertThat(response.getPolicies().size(), equalTo(numPolicies));

        for (int i = 0; i < numPolicies; i++) {
            String policyName = POLICY_NAME + i;
            client().execute(DeleteEnrichPolicyAction.INSTANCE, new DeleteEnrichPolicyAction.Request(policyName)).actionGet();
        }

        response = client().execute(ListEnrichPolicyAction.INSTANCE, new ListEnrichPolicyAction.Request()).actionGet();
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
        Settings settings = Settings.builder()
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .put(Node.NODE_DATA_SETTING.getKey(), false)
            .put(Node.NODE_INGEST_SETTING.getKey(), true)
            .build();
        String ingestOnlyNode = internalCluster().startNode(settings);

        List<String> keys = createSourceIndex(64);
        createAndExecutePolicy();
        createPipeline();
        enrich(keys, ingestOnlyNode);
    }

    private static void enrich(List<String> keys, String coordinatingNode) {
        int numDocs = 256;
        BulkRequest bulkRequest = new BulkRequest("my-index");
        for (int i = 0; i < numDocs; i++) {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.id(Integer.toString(i));
            indexRequest.setPipeline(PIPELINE_NAME);
            indexRequest.source(Collections.singletonMap(KEY_FIELD, randomFrom(keys)));
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
        assertThat("Expected no failure, but " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures(), is(false));

        for (int i = 0; i < numDocs; i++) {
            GetResponse getResponse = client().get(new GetRequest("my-index", Integer.toString(i))).actionGet();
            Map<String, Object> source = getResponse.getSourceAsMap();
            assertThat(source.size(), equalTo(1 + DECORATE_FIELDS.length));
            for (String field : DECORATE_FIELDS) {
                assertThat(source.get(field), notNullValue());
            }
        }
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
            indexRequest.source(mapOf(KEY_FIELD, key, DECORATE_FIELDS[0], randomAlphaOfLength(4),
                DECORATE_FIELDS[1], randomAlphaOfLength(4), DECORATE_FIELDS[2], randomAlphaOfLength(4)));
            client().index(indexRequest).actionGet();
        }
        client().admin().indices().refresh(new RefreshRequest(SOURCE_INDEX_NAME)).actionGet();
        return new ArrayList<>(keys);
    }

    private static void createAndExecutePolicy() {
        EnrichPolicy enrichPolicy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null,
            Arrays.asList(SOURCE_INDEX_NAME), KEY_FIELD, Arrays.asList(DECORATE_FIELDS));
        PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(POLICY_NAME, enrichPolicy);
        client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
        client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(POLICY_NAME)).actionGet();
    }

    private static void createPipeline() {
        String pipelineBody = "{\"processors\": [{\"enrich\": {\"policy_name\":\"" + POLICY_NAME +
            "\", \"enrich_values\": [{\"source\": \"" + DECORATE_FIELDS[0] + "\", \"target\": \"" + DECORATE_FIELDS[0] + "\"}," +
            "{\"source\": \"" + DECORATE_FIELDS[1] + "\", \"target\": \"" + DECORATE_FIELDS[1] + "\"}," +
            "{\"source\": \"" + DECORATE_FIELDS[2] + "\", \"target\": \"" + DECORATE_FIELDS[2] + "\"}" +
            "]}}]}";
        PutPipelineRequest request = new PutPipelineRequest(PIPELINE_NAME, new BytesArray(pipelineBody), XContentType.JSON);
        client().admin().cluster().putPipeline(request).actionGet();
    }

    private static  <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4) {
        Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        map.put(key4, value4);
        return map;
    }
}
