/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ListEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
public class EnrichMultiNodeIT extends ESIntegTestCase {

    private static final String POLICY_NAME = "my-policy";
    private static final String SOURCE_INDEX_NAME = "users";
    private static final String KEY_FIELD = "email";
    private static final String[] DECORATE_FIELDS = new String[]{"address", "city", "country"};

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(EnrichPlugin.class, ReindexPlugin.class);
    }

    public void testEnrichAPIs() {
        final int numPolicies = 4;
        internalCluster().startNodes(3);
        createSourceIndex();

        for (int i = 0; i < numPolicies; i++) {
            String policyName = POLICY_NAME + i;
            EnrichPolicy enrichPolicy =
                new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of(SOURCE_INDEX_NAME), KEY_FIELD, List.of(DECORATE_FIELDS));
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
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(8L));
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

    private static void createSourceIndex() {
        int numDocs = 8;
        for (int i = 0; i < numDocs; i++) {
            String key = randomAlphaOfLength(16);
            IndexRequest indexRequest = new IndexRequest(SOURCE_INDEX_NAME);
            indexRequest.create(true);
            indexRequest.id(key);
            indexRequest.source(Map.of(KEY_FIELD, key, DECORATE_FIELDS[0], randomAlphaOfLength(4),
                DECORATE_FIELDS[1], randomAlphaOfLength(4), DECORATE_FIELDS[2], randomAlphaOfLength(4)));
            client().index(indexRequest).actionGet();
        }
        client().admin().indices().refresh(new RefreshRequest(SOURCE_INDEX_NAME)).actionGet();
    }
}
