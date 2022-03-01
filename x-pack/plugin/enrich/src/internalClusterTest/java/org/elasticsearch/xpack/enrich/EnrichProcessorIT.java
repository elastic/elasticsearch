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

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.enrich.AbstractEnrichTestCase.createSourceIndices;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class EnrichProcessorIT extends ESSingleNodeTestCase {

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

    public void testEnrichCacheValuesCannotBeCorrupted() {
        // Ensure enrich cache is empty
        var statsRequest = new EnrichStatsAction.Request();
        var statsResponse = client().execute(EnrichStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(statsResponse.getCacheStats().size(), equalTo(1));
        assertThat(statsResponse.getCacheStats().get(0).getCount(), equalTo(0L));
        assertThat(statsResponse.getCacheStats().get(0).getMisses(), equalTo(0L));
        assertThat(statsResponse.getCacheStats().get(0).getHits(), equalTo(0L));

        String policyName = "device-enrich-policy";
        String sourceIndexName = "devices-idx";

        var enrichPolicy = new EnrichPolicy(
            EnrichPolicy.MATCH_TYPE,
            null,
            List.of(sourceIndexName),
            "host.ip",
            List.of("device.name", "host.ip")
        );

        // Create source index and add a single document:
        createSourceIndices(client(), enrichPolicy);
        IndexRequest indexRequest = new IndexRequest(sourceIndexName);
        indexRequest.create(true);
        indexRequest.source("""
            {
              "host": {
                "ip": "10.151.80.8"
              },
              "device": {
                "name": "bla"
              }
            }
            """, XContentType.JSON);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client().index(indexRequest).actionGet();

        // Store policy and execute it:
        var putPolicyRequest = new PutEnrichPolicyAction.Request(policyName, enrichPolicy);
        client().execute(PutEnrichPolicyAction.INSTANCE, putPolicyRequest).actionGet();
        var executePolicyRequest = new ExecuteEnrichPolicyAction.Request(policyName);
        client().execute(ExecuteEnrichPolicyAction.INSTANCE, executePolicyRequest).actionGet();

        var simulatePipelineRequest = new SimulatePipelineRequest(new BytesArray("""
            {
              "pipeline": {
                "processors": [
                  {
                    "enrich": {
                      "policy_name": "device-enrich-policy",
                      "field": "host.ip",
                      "target_field": "_tmp.device"
                    }
                  },
                  {
                    "rename" : {
                      "field" : "_tmp.device.device.name",
                      "target_field" : "device.name"
                    }
                  }
                ]
              },
              "docs": [
                {
                  "_source": {
                    "host": {
                      "ip": "10.151.80.8"
                    }
                  }
                }
              ]
            }
            """), XContentType.JSON);
        var response = client().admin().cluster().simulatePipeline(simulatePipelineRequest).actionGet();
        var result = (SimulateDocumentBaseResult) response.getResults().get(0);
        assertThat(result.getFailure(), nullValue());
        assertThat(result.getIngestDocument().getFieldValue("device.name", String.class), equalTo("bla"));

        // Verify that there was a cache miss and a new entry was added to enrich cache.
        statsResponse = client().execute(EnrichStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(statsResponse.getCacheStats().size(), equalTo(1));
        assertThat(statsResponse.getCacheStats().get(0).getCount(), equalTo(1L));
        assertThat(statsResponse.getCacheStats().get(0).getMisses(), equalTo(1L));
        assertThat(statsResponse.getCacheStats().get(0).getHits(), equalTo(0L));

        simulatePipelineRequest = new SimulatePipelineRequest(new BytesArray("""
            {
              "pipeline": {
                "processors": [
                  {
                    "enrich": {
                      "policy_name": "device-enrich-policy",
                      "field": "host.ip",
                      "target_field": "_tmp"
                    }
                  }
                ]
              },
              "docs": [
                {
                  "_source": {
                    "host": {
                      "ip": "10.151.80.8"
                    }
                  }
                }
              ]
            }
            """), XContentType.JSON);
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
