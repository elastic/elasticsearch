/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.SimulateDocumentBaseResult;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.IngestPipelineTestUtils.jsonSimulatePipelineRequest;
import static org.elasticsearch.xpack.enrich.AbstractEnrichTestCase.createSourceIndices;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class EnrichProcessorMaxMatchesIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateEnrich.class, ReindexPlugin.class, IngestCommonPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            // TODO Change this to run with security enabled
            // https://github.com/elastic/elasticsearch/issues/75940
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .build();
    }

    public void testEnrichCacheValuesAndMaxMatches() {
        // this test is meant to be much less ignorable than a mere comment in the code, since the behavior here is tricky.

        // there's an interesting edge case where two processors could be using the same policy and search, etc,
        // but that they have a different number of max_matches -- if we're not careful about how we implement caching,
        // then we could miss that edge case and return the wrong results from the cache.

        // Ensure enrich cache is empty
        var statsRequest = new EnrichStatsAction.Request(TEST_REQUEST_TIMEOUT);
        var statsResponse = client().execute(EnrichStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(statsResponse.getCacheStats().size(), equalTo(1));
        assertThat(statsResponse.getCacheStats().get(0).count(), equalTo(0L));
        assertThat(statsResponse.getCacheStats().get(0).misses(), equalTo(0L));
        assertThat(statsResponse.getCacheStats().get(0).hits(), equalTo(0L));

        String policyName = "kv";
        String sourceIndexName = "kv";

        var enrichPolicy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndexName), "key", List.of("value"));

        // Create source index and add two documents:
        createSourceIndices(client(), enrichPolicy);
        {
            IndexRequest indexRequest = new IndexRequest(sourceIndexName);
            indexRequest.create(true);
            indexRequest.source("""
                {
                  "key": "k1",
                  "value": "v1"
                }
                """, XContentType.JSON);
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client().index(indexRequest).actionGet();
        }
        {
            IndexRequest indexRequest = new IndexRequest(sourceIndexName);
            indexRequest.create(true);
            indexRequest.source("""
                {
                  "key": "k1",
                  "value": "v2"
                }
                """, XContentType.JSON);
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client().index(indexRequest).actionGet();
        }

        // Store policy and execute it:
        var putPolicyRequest = new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName, enrichPolicy);
        client().execute(PutEnrichPolicyAction.INSTANCE, putPolicyRequest).actionGet();
        var executePolicyRequest = new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName);
        client().execute(ExecuteEnrichPolicyAction.INSTANCE, executePolicyRequest).actionGet();

        {
            // run a single enrich processor to fill the cache, note that the default max_matches is 1 (so it's not given explicitly here)
            var simulatePipelineRequest = jsonSimulatePipelineRequest("""
                {
                  "pipeline": {
                    "processors" : [
                      {
                        "enrich": {
                          "policy_name": "kv",
                          "field": "key",
                          "target_field": "result"
                        }
                      }
                    ]
                  },
                  "docs": [
                    {
                      "_source": {
                        "key": "k1"
                      }
                    }
                  ]
                }
                """);
            var response = clusterAdmin().simulatePipeline(simulatePipelineRequest).actionGet();
            var result = (SimulateDocumentBaseResult) response.getResults().get(0);
            assertThat(result.getFailure(), nullValue());
            // it's not actually important in this specific test whether the result is v1 or v2
            assertThat(result.getIngestDocument().getFieldValue("result.value", String.class), containsString("v"));
        }

        {
            // run two enrich processors with different max_matches, and see if we still get the right behavior
            var simulatePipelineRequest = jsonSimulatePipelineRequest("""
                {
                  "pipeline": {
                    "processors" : [
                      {
                        "enrich": {
                          "policy_name": "kv",
                          "field": "key",
                          "target_field": "result"
                        }
                      },
                      {
                        "enrich": {
                          "policy_name": "kv",
                          "field": "key",
                          "target_field": "results",
                          "max_matches": 8
                        }
                      }
                    ]
                  },
                  "docs": [
                    {
                      "_source": {
                        "key": "k1"
                      }
                    }
                  ]
                }
                """);
            var response = clusterAdmin().simulatePipeline(simulatePipelineRequest).actionGet();
            var result = (SimulateDocumentBaseResult) response.getResults().get(0);
            assertThat(result.getFailure(), nullValue());
            // it's not actually important in this specific test whether the result is v1 or v2
            assertThat(result.getIngestDocument().getFieldValue("result.value", String.class), containsString("v"));

            // this is the important part of the test -- did the max_matches=1 case pollute the cache for the max_matches=8 case?
            @SuppressWarnings("unchecked")
            List<Map<String, String>> results = (List<Map<String, String>>) result.getIngestDocument().getSource().get("results");
            List<String> values = results.stream().map(m -> m.get("value")).toList();
            // if these assertions fail, it probably means you were fussing about with the EnrichCache.CacheKey and tried removing
            // the max_matches accounting from it
            assertThat(values, containsInAnyOrder("v1", "v2"));
            assertThat(values, hasSize(2));
        }

        statsResponse = client().execute(EnrichStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(statsResponse.getCacheStats().size(), equalTo(1));
        // there are two items in the cache, the single result from max_matches 1 (implied), and the multi-result from max_matches 8
        assertThat(statsResponse.getCacheStats().get(0).count(), equalTo(2L));
    }

}
