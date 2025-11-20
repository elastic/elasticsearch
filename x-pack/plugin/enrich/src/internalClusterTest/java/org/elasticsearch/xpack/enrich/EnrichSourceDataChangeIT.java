/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
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
import java.util.Map;

import static org.elasticsearch.xpack.enrich.AbstractEnrichTestCase.createSourceIndices;
import static org.hamcrest.Matchers.equalTo;

public class EnrichSourceDataChangeIT extends ESSingleNodeTestCase {

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

    public void testChangesToTheSourceIndexTakeEffectOnPolicyExecution() throws Exception {
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

        final String initialDeviceName = "some.device." + randomAlphaOfLength(10);

        // add a single document to the enrich index
        setEnrichDeviceName(initialDeviceName);

        // store the enrich policy and execute it
        var putPolicyRequest = new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName, enrichPolicy);
        client().execute(PutEnrichPolicyAction.INSTANCE, putPolicyRequest).actionGet();
        executeEnrichPolicy();

        // create an honest to goodness pipeline for repeated executions (we're not running any _simulate requests here)
        final String pipelineName = "my-pipeline";
        putJsonPipeline(pipelineName, """
            {
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
                  },
                  {
                    "remove" : {
                      "field" : "_tmp"
                    }
                  }
              ]
            }""");

        {
            final var indexRequest = new IndexRequest(sourceIndexName);
            indexRequest.id("1");
            indexRequest.setPipeline("my-pipeline");
            indexRequest.source("""
                {
                  "host": {
                    "ip": "10.151.80.8"
                  }
                }
                """, XContentType.JSON);
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client().index(indexRequest).actionGet();

            final var response = client().get(new GetRequest(sourceIndexName).id("1")).actionGet();
            assertThat(response.getSource().get("device"), equalTo(Map.of("name", initialDeviceName)));
        }

        // add different document to the enrich index
        final String changedDeviceName = "some.device." + randomAlphaOfLength(10);
        setEnrichDeviceName(changedDeviceName);

        // execute the policy to pick up the change
        executeEnrichPolicy();

        // it can take a moment for the execution to take effect, so assertBusy
        assertBusy(() -> {
            final var indexRequest = new IndexRequest(sourceIndexName);
            indexRequest.id("2");
            indexRequest.setPipeline("my-pipeline");
            indexRequest.source("""
                {
                  "host": {
                    "ip": "10.151.80.8"
                  }
                }
                """, XContentType.JSON);
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client().index(indexRequest).actionGet();

            final var response = client().get(new GetRequest(sourceIndexName).id("2")).actionGet();
            assertThat(response.getSource().get("device"), equalTo(Map.of("name", changedDeviceName)));
        });
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
