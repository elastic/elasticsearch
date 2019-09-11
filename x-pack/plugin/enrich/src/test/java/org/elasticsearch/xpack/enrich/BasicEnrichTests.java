/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

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
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.enrich.EnrichMultiNodeIT.DECORATE_FIELDS;
import static org.elasticsearch.xpack.enrich.EnrichMultiNodeIT.MATCH_FIELD;
import static org.elasticsearch.xpack.enrich.EnrichMultiNodeIT.SOURCE_INDEX_NAME;
import static org.elasticsearch.xpack.enrich.MatchProcessorTests.mapOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class BasicEnrichTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateEnrich.class, ReindexPlugin.class);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testIngestDataWithEnrichProcessor() {
        int numDocs = 32;
        int maxMatches = randomIntBetween(2, 8);
        List<String> keys = createSourceIndex(numDocs, maxMatches);

        String policyName = "my-policy";
        EnrichPolicy enrichPolicy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null,
            Arrays.asList(SOURCE_INDEX_NAME), MATCH_FIELD, Arrays.asList(DECORATE_FIELDS));
        PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(policyName, enrichPolicy);
        client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
        client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(policyName)).actionGet();

        String pipelineName = "my-pipeline";
        String pipelineBody = "{\"processors\": [{\"enrich\": {\"policy_name\":\"" + policyName +
            "\", \"field\": \"" + MATCH_FIELD + "\", \"target_field\": \"users\", \"max_matches\": " + maxMatches + "}}]}";
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest(pipelineName, new BytesArray(pipelineBody), XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).actionGet();

        BulkRequest bulkRequest = new BulkRequest("my-index");
        for (int i = 0; i < numDocs; i++) {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.id(Integer.toString(i));
            indexRequest.setPipeline(pipelineName);
            indexRequest.source(Collections.singletonMap(MATCH_FIELD, keys.get(i)));
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

        EnrichStatsAction.Response statsResponse =
            client().execute(EnrichStatsAction.INSTANCE, new EnrichStatsAction.Request()).actionGet();
        assertThat(statsResponse.getCoordinatorStats().size(), equalTo(1));
        String localNodeId = getInstanceFromNode(ClusterService.class).localNode().getId();
        assertThat(statsResponse.getCoordinatorStats().get(localNodeId).getRemoteRequestsTotal(), greaterThanOrEqualTo(1L));
        assertThat(statsResponse.getCoordinatorStats().get(localNodeId).getExecutedSearchesTotal(), equalTo((long) numDocs));
    }

    public void testMultiplePolicies() {
        int numPolicies = 8;
        for (int i = 0; i < numPolicies; i++) {
            String policyName = "policy" + i;

            IndexRequest indexRequest = new IndexRequest("source-" + i);
            indexRequest.source("key", "key", "value", "val" + i);
            client().index(indexRequest).actionGet();
            client().admin().indices().refresh(new RefreshRequest("source-" + i)).actionGet();

            EnrichPolicy enrichPolicy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null,
                Collections.singletonList("source-" + i), "key", Collections.singletonList("value"));
            PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(policyName, enrichPolicy);
            client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
            client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(policyName)).actionGet();

            String pipelineName = "pipeline" + i;
            String pipelineBody = "{\"processors\": [{\"enrich\": {\"policy_name\":\"" + policyName +
                "\", \"field\": \"key\", \"target_field\": \"target\"}}]}";
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest(pipelineName, new BytesArray(pipelineBody), XContentType.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).actionGet();
        }

        BulkRequest bulkRequest = new BulkRequest("my-index");
        for (int i = 0; i < numPolicies; i++) {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.id(Integer.toString(i));
            indexRequest.setPipeline("pipeline" + i);
            indexRequest.source(Collections.singletonMap("key", "key"));
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat("Expected no failure, but " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures(), is(false));

        for (int i = 0; i < numPolicies; i++) {
            GetResponse getResponse = client().get(new GetRequest("my-index", Integer.toString(i))).actionGet();
            Map<String, Object> source = getResponse.getSourceAsMap();
            assertThat(source.size(), equalTo(2));
            assertThat(source.get("target"), equalTo(Arrays.asList(mapOf("key", "key", "value", "val" + i))));
        }
    }

    private List<String> createSourceIndex(int numKeys, int numDocsPerKey) {
        Set<String> keys = new HashSet<>();
        for (int id = 0; id < numKeys; id++) {
            String key;
            do {
                key = randomAlphaOfLength(16);
            } while (keys.add(key) == false);

            for (int doc = 0; doc < numDocsPerKey; doc++) {
                IndexRequest indexRequest = new IndexRequest(SOURCE_INDEX_NAME);
                indexRequest.source(mapOf(MATCH_FIELD, key, DECORATE_FIELDS[0], key + "0",
                    DECORATE_FIELDS[1], key + "1", DECORATE_FIELDS[2], key + "2"));
                client().index(indexRequest).actionGet();
            }
        }
        client().admin().indices().refresh(new RefreshRequest(SOURCE_INDEX_NAME)).actionGet();
        return new ArrayList<>(keys);
    }

}
