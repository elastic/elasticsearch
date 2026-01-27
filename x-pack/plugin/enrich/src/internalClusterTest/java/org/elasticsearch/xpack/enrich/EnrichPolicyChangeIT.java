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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.ingest.IngestPipelineTestUtils.jsonSimulatePipelineRequest;
import static org.elasticsearch.xpack.enrich.AbstractEnrichTestCase.createSourceIndices;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class EnrichPolicyChangeIT extends ESSingleNodeTestCase {

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

    private final String policyName = "device-enrich-policy";
    private final String sourceIndexName = "devices-idx";

    public void testEnrichCacheValuesCannotBeCorrupted() throws Exception {
        // create and store the enrich policy
        final var enrichPolicy = new EnrichPolicy(
            EnrichPolicy.MATCH_TYPE,
            null,
            List.of(sourceIndexName),
            "host.ip",
            List.of("device.name", "host.ip")
        );

        // create the source index
        createSourceIndices(client(), enrichPolicy);

        // add a single document to the enrich index
        setEnrichDeviceName("some.device." + randomAlphaOfLength(10));

        // store the enrich policy
        var putPolicyRequest = new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName, enrichPolicy);
        client().execute(PutEnrichPolicyAction.INSTANCE, putPolicyRequest).actionGet();

        // execute the policy once
        executeEnrichPolicy();

        // add a low priority cluster state applier to increase the odds of a race occurring between
        // the cluster state *appliers* having been run (this adjusts the enrich index pointer) and the
        // cluster state *listeners* having been run (which adjusts the alias and therefore the search results)
        final var clusterService = node().injector().getInstance(ClusterService.class);
        clusterService.addLowPriorityApplier((event) -> safeSleep(10));

        // kick off some threads that just bang on _simulate in the background
        final var finished = new AtomicBoolean(false);
        for (int i = 0; i < 5; i++) {
            new Thread(() -> {
                while (finished.get() == false) {
                    simulatePipeline();
                }
            }).start();
        }

        try {
            for (int i = 0; i < randomIntBetween(10, 100); i++) {
                final String deviceName = "some.device." + randomAlphaOfLength(10);

                // add a single document to the enrich index
                setEnrichDeviceName(deviceName);

                // execute the policy
                executeEnrichPolicy();

                // simulate the pipeline and confirm that we see the expected result
                assertBusy(() -> {
                    var result = simulatePipeline();
                    assertThat(result.getFailure(), nullValue());
                    assertThat(result.getIngestDocument().getFieldValue("device.name", String.class), equalTo(deviceName));
                });
            }
        } finally {
            // we're finished, so those threads can all quit now
            finished.set(true);
        }
    }

    private SimulateDocumentBaseResult simulatePipeline() {
        final var simulatePipelineRequest = jsonSimulatePipelineRequest("""
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
            """);
        final var response = clusterAdmin().simulatePipeline(simulatePipelineRequest).actionGet();
        return (SimulateDocumentBaseResult) response.getResults().getFirst();
    }

    private void setEnrichDeviceName(final String deviceName) {
        final var indexRequest = new IndexRequest(sourceIndexName);
        indexRequest.id("1"); // there's only one document, and we keep overwriting it
        indexRequest.source(Strings.format("""
            {
              "host": {
                "ip": "10.151.80.8"
              },
              "device": {
                "name": "%s"
              }
            }
            """, deviceName), XContentType.JSON);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client().index(indexRequest).actionGet();
    }

    private void executeEnrichPolicy() {
        final var executePolicyRequest = new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName);
        client().execute(ExecuteEnrichPolicyAction.INSTANCE, executePolicyRequest).actionGet();
    }

}
