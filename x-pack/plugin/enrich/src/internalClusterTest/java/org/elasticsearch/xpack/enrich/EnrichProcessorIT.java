/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.SimulateDocumentBaseResult;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesArray;
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.xpack.enrich.AbstractEnrichTestCase.createSourceIndices;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class EnrichProcessorIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateEnrich.class, ReindexPlugin.class, IngestCommonPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            // TODO Change this to run with security enabled
            // https://github.com/elastic/elasticsearch/issues/75940
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .build();
    }

    public void testEnrichCacheValuesCannotBeCorrupted() {
        // Ensure enrich cache is empty
        EnrichStatsAction.Request statsRequest = new EnrichStatsAction.Request();
        EnrichStatsAction.Response statsResponse = client().execute(EnrichStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(statsResponse.getCacheStats().size(), equalTo(1));
        assertThat(statsResponse.getCacheStats().get(0).getCount(), equalTo(0L));
        assertThat(statsResponse.getCacheStats().get(0).getMisses(), equalTo(0L));
        assertThat(statsResponse.getCacheStats().get(0).getHits(), equalTo(0L));

        String policyName = "device-enrich-policy";
        String sourceIndexName = "devices-idx";

        EnrichPolicy enrichPolicy = new EnrichPolicy(
            EnrichPolicy.MATCH_TYPE,
            null,
            Collections.singletonList(sourceIndexName),
            "host.ip",
            Arrays.asList("device.name", "host.ip")
        );

        // Create source index and add a single document:
        createSourceIndices(client(), enrichPolicy);
        IndexRequest indexRequest = new IndexRequest(sourceIndexName);
        indexRequest.create(true);
        indexRequest.source("{\"host\": {\"ip\": \"10.151.80.8\"},\"device\": {\"name\": \"bla\"}}", XContentType.JSON);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client().index(indexRequest).actionGet();

        // Store policy and execute it:
        PutEnrichPolicyAction.Request putPolicyRequest = new PutEnrichPolicyAction.Request(policyName, enrichPolicy);
        client().execute(PutEnrichPolicyAction.INSTANCE, putPolicyRequest).actionGet();
        ExecuteEnrichPolicyAction.Request executePolicyRequest = new ExecuteEnrichPolicyAction.Request(policyName);
        client().execute(ExecuteEnrichPolicyAction.INSTANCE, executePolicyRequest).actionGet();

        SimulatePipelineRequest simulatePipelineRequest = new SimulatePipelineRequest(
            new BytesArray(
                "{\n"
                    + "              \"pipeline\": {\n"
                    + "                \"processors\": [\n"
                    + "                  {\n"
                    + "                    \"enrich\": {\n"
                    + "                      \"policy_name\": \"device-enrich-policy\",\n"
                    + "                      \"field\": \"host.ip\",\n"
                    + "                      \"target_field\": \"_tmp.device\"\n"
                    + "                    }\n"
                    + "                  },\n"
                    + "                  {\n"
                    + "                    \"rename\" : {\n"
                    + "                      \"field\" : \"_tmp.device.device.name\",\n"
                    + "                      \"target_field\" : \"device.name\"\n"
                    + "                    }\n"
                    + "                  }\n"
                    + "                ]\n"
                    + "              },\n"
                    + "              \"docs\": [\n"
                    + "                {\n"
                    + "                  \"_source\": {\n"
                    + "                    \"host\": {\n"
                    + "                      \"ip\": \"10.151.80.8\"\n"
                    + "                    }\n"
                    + "                  }\n"
                    + "                }\n"
                    + "              ]\n"
                    + "            }"
            ),
            XContentType.JSON
        );
        SimulatePipelineResponse response = client().admin().cluster().simulatePipeline(simulatePipelineRequest).actionGet();
        SimulateDocumentBaseResult result = (SimulateDocumentBaseResult) response.getResults().get(0);
        assertThat(result.getFailure(), nullValue());
        assertThat(result.getIngestDocument().getFieldValue("device.name", String.class), equalTo("bla"));

        // Verify that there was a cache miss and a new entry was added to enrich cache.
        statsResponse = client().execute(EnrichStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(statsResponse.getCacheStats().size(), equalTo(1));
        assertThat(statsResponse.getCacheStats().get(0).getCount(), equalTo(1L));
        assertThat(statsResponse.getCacheStats().get(0).getMisses(), equalTo(1L));
        assertThat(statsResponse.getCacheStats().get(0).getHits(), equalTo(0L));

        simulatePipelineRequest = new SimulatePipelineRequest(
            new BytesArray(
                "{\n"
                    + "              \"pipeline\": {\n"
                    + "                \"processors\": [\n"
                    + "                  {\n"
                    + "                    \"enrich\": {\n"
                    + "                      \"policy_name\": \"device-enrich-policy\",\n"
                    + "                      \"field\": \"host.ip\",\n"
                    + "                      \"target_field\": \"_tmp\"\n"
                    + "                    }\n"
                    + "                  }\n"
                    + "                ]\n"
                    + "              },\n"
                    + "              \"docs\": [\n"
                    + "                {\n"
                    + "                  \"_source\": {\n"
                    + "                    \"host\": {\n"
                    + "                      \"ip\": \"10.151.80.8\"\n"
                    + "                    }\n"
                    + "                  }\n"
                    + "                }\n"
                    + "              ]\n"
                    + "            }"
            ),
            XContentType.JSON
        );
        response = client().admin().cluster().simulatePipeline(simulatePipelineRequest).actionGet();
        result = (SimulateDocumentBaseResult) response.getResults().get(0);
        assertThat(result.getFailure(), nullValue());
        assertThat(result.getIngestDocument().getFieldValue("_tmp.device.name", String.class), equalTo("bla"));

        // Verify that enrich lookup was served from cache:
        statsResponse = client().execute(EnrichStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(statsResponse.getCacheStats().size(), equalTo(1));
        assertThat(statsResponse.getCacheStats().get(0).getCount(), equalTo(1L));
        assertThat(statsResponse.getCacheStats().get(0).getMisses(), equalTo(1L));
        assertThat(statsResponse.getCacheStats().get(0).getHits(), equalTo(1L));
    }

}
