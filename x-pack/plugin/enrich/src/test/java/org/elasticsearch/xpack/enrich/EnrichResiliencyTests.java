/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EnrichResiliencyTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(ReindexPlugin.class, IngestCommonPlugin.class, LocalStateEnrich.class);
    }

    @Override
    protected Settings nodeSettings() {
        // Severely throttle the processing throughput to reach max capacity easier
        return Settings.builder()
            .put(EnrichPlugin.COORDINATOR_PROXY_MAX_CONCURRENT_REQUESTS.getKey(), 1)
            .put(EnrichPlugin.COORDINATOR_PROXY_MAX_LOOKUPS_PER_REQUEST.getKey(), 1)
            .put(EnrichPlugin.COORDINATOR_PROXY_QUEUE_CAPACITY.getKey(), 10)
            // TODO Fix the test so that it runs with security enabled
            // https://github.com/elastic/elasticsearch/issues/75940
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .build();
    }

    public void testWriteThreadLivenessBackToBack() throws Exception {
        ensureGreen();

        long testSuffix = System.currentTimeMillis();
        String enrichIndexName = "enrich_lookup_" + testSuffix;
        String enrichPolicyName = "enrich_policy_" + testSuffix;
        String enrichPipelineName = "enrich_pipeline_" + testSuffix;
        String enrichedIndexName = "enrich_results_" + testSuffix;

        client().index(
            new IndexRequest(enrichIndexName).source(
                JsonXContent.contentBuilder().startObject().field("my_key", "key").field("my_value", "data").endObject()
            )
        ).actionGet();

        client().admin().indices().refresh(new RefreshRequest(enrichIndexName)).actionGet();

        client().execute(
            PutEnrichPolicyAction.INSTANCE,
            new PutEnrichPolicyAction.Request(
                enrichPolicyName,
                new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(enrichIndexName), "my_key", List.of("my_value"))
            )
        ).actionGet();

        client().execute(
            ExecuteEnrichPolicyAction.INSTANCE,
            new ExecuteEnrichPolicyAction.Request(enrichPolicyName).setWaitForCompletion(true)
        ).actionGet();

        XContentBuilder pipe1 = JsonXContent.contentBuilder();
        pipe1.startObject();
        {
            pipe1.startArray("processors");
            {
                pipe1.startObject();
                {
                    pipe1.startObject("enrich");
                    {
                        pipe1.field("policy_name", enrichPolicyName);
                        pipe1.field("field", "custom_id");
                        pipe1.field("target_field", "enrich_value_1");
                    }
                    pipe1.endObject();
                }
                pipe1.endObject();
                pipe1.startObject();
                {
                    pipe1.startObject("enrich");
                    {
                        pipe1.field("policy_name", enrichPolicyName);
                        pipe1.field("field", "custom_id");
                        pipe1.field("target_field", "enrich_value_2");
                    }
                    pipe1.endObject();
                }
                pipe1.endObject();
            }
            pipe1.endArray();
        }
        pipe1.endObject();

        client().execute(
            PutPipelineAction.INSTANCE,
            new PutPipelineRequest(enrichPipelineName, BytesReference.bytes(pipe1), XContentType.JSON)
        ).actionGet();

        client().admin().indices().create(new CreateIndexRequest(enrichedIndexName)).actionGet();

        XContentBuilder doc = JsonXContent.contentBuilder().startObject().field("custom_id", "key").endObject();

        BulkRequest bulk = new BulkRequest(enrichedIndexName);
        bulk.timeout(new TimeValue(10, TimeUnit.SECONDS));
        for (int idx = 0; idx < 50; idx++) {
            bulk.add(new IndexRequest().source(doc).setPipeline(enrichPipelineName));
        }

        BulkResponse bulkItemResponses = client().bulk(bulk).actionGet(new TimeValue(30, TimeUnit.SECONDS));

        assertTrue(bulkItemResponses.hasFailures());
        BulkItemResponse.Failure firstFailure = null;
        int successfulItems = 0;
        for (BulkItemResponse item : bulkItemResponses.getItems()) {
            if (item.isFailed() && firstFailure == null) {
                firstFailure = item.getFailure();
            } else if (item.isFailed() == false) {
                successfulItems++;
            }
        }
        assertNotNull(firstFailure);
        assertThat(firstFailure.getStatus().getStatus(), is(equalTo(429)));
        assertThat(firstFailure.getMessage(), containsString("Could not perform enrichment, enrich coordination queue at capacity"));

        client().admin().indices().refresh(new RefreshRequest(enrichedIndexName)).actionGet();
        assertEquals(successfulItems, client().search(new SearchRequest(enrichedIndexName)).actionGet().getHits().getTotalHits().value);
    }

    public void testWriteThreadLivenessWithPipeline() throws Exception {
        ensureGreen();

        long testSuffix = System.currentTimeMillis();
        String enrichIndexName = "enrich_lookup_" + testSuffix;
        String enrichPolicyName = "enrich_policy_" + testSuffix;
        String enrichPipelineName = "enrich_pipeline_" + testSuffix;
        String enrichedIndexName = "enrich_results_" + testSuffix;
        String enrichPipelineName1 = enrichPipelineName + "_1";
        String enrichPipelineName2 = enrichPipelineName + "_2";

        client().index(
            new IndexRequest(enrichIndexName).source(
                JsonXContent.contentBuilder().startObject().field("my_key", "key").field("my_value", "data").endObject()
            )
        ).actionGet();

        client().admin().indices().refresh(new RefreshRequest(enrichIndexName)).actionGet();

        client().execute(
            PutEnrichPolicyAction.INSTANCE,
            new PutEnrichPolicyAction.Request(
                enrichPolicyName,
                new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(enrichIndexName), "my_key", List.of("my_value"))
            )
        ).actionGet();

        client().execute(
            ExecuteEnrichPolicyAction.INSTANCE,
            new ExecuteEnrichPolicyAction.Request(enrichPolicyName).setWaitForCompletion(true)
        ).actionGet();

        XContentBuilder pipe1 = JsonXContent.contentBuilder();
        pipe1.startObject();
        {
            pipe1.startArray("processors");
            {
                pipe1.startObject();
                {
                    pipe1.startObject("enrich");
                    {
                        pipe1.field("policy_name", enrichPolicyName);
                        pipe1.field("field", "custom_id");
                        pipe1.field("target_field", "enrich_value_1");
                    }
                    pipe1.endObject();
                }
                pipe1.endObject();
                pipe1.startObject();
                {
                    pipe1.startObject("pipeline");
                    {
                        pipe1.field("name", enrichPipelineName2);
                    }
                    pipe1.endObject();
                }
                pipe1.endObject();
            }
            pipe1.endArray();
        }
        pipe1.endObject();

        XContentBuilder pipe2 = JsonXContent.contentBuilder();
        pipe2.startObject();
        {
            pipe2.startArray("processors");
            {
                pipe2.startObject();
                {
                    pipe2.startObject("enrich");
                    {
                        pipe2.field("policy_name", enrichPolicyName);
                        pipe2.field("field", "custom_id");
                        pipe2.field("target_field", "enrich_value_2");
                    }
                    pipe2.endObject();
                }
                pipe2.endObject();
            }
            pipe2.endArray();
        }
        pipe2.endObject();

        client().execute(
            PutPipelineAction.INSTANCE,
            new PutPipelineRequest(enrichPipelineName1, BytesReference.bytes(pipe1), XContentType.JSON)
        ).actionGet();

        client().execute(
            PutPipelineAction.INSTANCE,
            new PutPipelineRequest(enrichPipelineName2, BytesReference.bytes(pipe2), XContentType.JSON)
        ).actionGet();

        client().admin().indices().create(new CreateIndexRequest(enrichedIndexName)).actionGet();

        XContentBuilder doc = JsonXContent.contentBuilder().startObject().field("custom_id", "key").endObject();

        BulkRequest bulk = new BulkRequest(enrichedIndexName);
        bulk.timeout(new TimeValue(10, TimeUnit.SECONDS));
        for (int idx = 0; idx < 50; idx++) {
            bulk.add(new IndexRequest().source(doc).setPipeline(enrichPipelineName1));
        }

        BulkResponse bulkItemResponses = client().bulk(bulk).actionGet(new TimeValue(30, TimeUnit.SECONDS));

        assertTrue(bulkItemResponses.hasFailures());
        BulkItemResponse.Failure firstFailure = null;
        int successfulItems = 0;
        for (BulkItemResponse item : bulkItemResponses.getItems()) {
            if (item.isFailed() && firstFailure == null) {
                firstFailure = item.getFailure();
            } else if (item.isFailed() == false) {
                successfulItems++;
            }
        }
        assertNotNull(firstFailure);
        assertThat(firstFailure.getStatus().getStatus(), is(equalTo(429)));
        assertThat(firstFailure.getMessage(), containsString("Could not perform enrichment, enrich coordination queue at capacity"));

        client().admin().indices().refresh(new RefreshRequest(enrichedIndexName)).actionGet();
        assertEquals(successfulItems, client().search(new SearchRequest(enrichedIndexName)).actionGet().getHits().getTotalHits().value);
    }
}
