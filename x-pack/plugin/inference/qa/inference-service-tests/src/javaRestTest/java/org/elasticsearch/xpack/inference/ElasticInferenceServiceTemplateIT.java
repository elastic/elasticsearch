/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.completion.UnifiedCompletionUtils;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.http.MockResponse;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.getModel;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Exercises the two Elasticsearch responsibilities for EIS prompt templates against a mock EIS gateway: the template policy an
 * endpoint advertises in its authorization response is stored and shown by {@code GET _inference}, and a chat completion request
 * carrying a {@code template} reaches EIS unchanged. The template request targets the endpoint that requires templates, but any
 * EIS chat completion endpoint accepts one; the policy only decides whether {@code messages} are rejected.
 */
public class ElasticInferenceServiceTemplateIT extends BaseEisMockGatewayRestTest {

    private static final String TEMPLATE_REQUIRED_ENDPOINT_ID = ".template-required-chat_completion";
    private static final String TEMPLATE_REQUIRED_MODEL = "template-required";
    private static final String GENERAL_ENDPOINT_ID = ".general-chat_completion";

    private static final String AUTHORIZATION_RESPONSE = """
        {
          "inference_endpoints": [
            {
              "id": ".template-required-chat_completion",
              "model_name": "template-required",
              "task_types": { "eis": "chat", "elasticsearch": "chat_completion" },
              "status": "ga",
              "release_date": "2026-01-01",
              "regions": [],
              "fingerprint": "template-required-fingerprint",
              "capabilities": {
                "templates": { "policy": "required" }
              }
            },
            {
              "id": ".general-chat_completion",
              "model_name": "general",
              "task_types": { "eis": "chat", "elasticsearch": "chat_completion" },
              "status": "ga",
              "release_date": "2026-01-01",
              "regions": [],
              "fingerprint": "general-fingerprint"
            }
          ]
        }
        """;

    private static final MockElasticInferenceServiceAuthorizationServer mockEISServer =
        new MockElasticInferenceServiceAuthorizationServer();

    static {
        // Consumed by the node's single authorization request at startup, before any test runs.
        mockEISServer.getWebServer().enqueue(new MockResponse().setResponseCode(200).setBody(AUTHORIZATION_RESPONSE));
    }

    private static final ElasticsearchCluster cluster = eisClusterBuilder(mockEISServer).setting("xpack.security.enabled", "false").build();

    // The mock server must start before the cluster so its URL is bound when the cluster reads the setting.
    @ClassRule
    public static final TestRule ruleChain = RuleChain.outerRule(mockEISServer).around(cluster);

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @SuppressWarnings("unchecked")
    public void testGetEndpoint_ShowsTemplatePolicy() throws Exception {
        awaitPreconfiguredEndpoint(TEMPLATE_REQUIRED_ENDPOINT_ID);

        var templateRequiredEndpoint = getModel(TEMPLATE_REQUIRED_ENDPOINT_ID);
        var metadata = (Map<String, Object>) templateRequiredEndpoint.get(EndpointMetadata.METADATA_FIELD_NAME);
        var capabilities = (Map<String, Object>) metadata.get(EndpointMetadata.CAPABILITIES_FIELD_NAME);
        var templates = (Map<String, Object>) capabilities.get(EndpointMetadata.Capabilities.TEMPLATES_FIELD_NAME);
        assertThat(templates.get(EndpointMetadata.TemplatesCapability.POLICY_FIELD_NAME), is("required"));

        var generalEndpoint = getModel(GENERAL_ENDPOINT_ID);
        var generalMetadata = (Map<String, Object>) generalEndpoint.get(EndpointMetadata.METADATA_FIELD_NAME);
        assertThat(generalMetadata.get(EndpointMetadata.CAPABILITIES_FIELD_NAME), nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testTemplateRequest_IsForwardedToEis() throws Exception {
        awaitPreconfiguredEndpoint(TEMPLATE_REQUIRED_ENDPOINT_ID);

        mockEISServer.getWebServer().enqueue(streamResponse(TEMPLATE_REQUIRED_MODEL));
        executeStreamRequest(TEMPLATE_REQUIRED_ENDPOINT_ID, XContentHelper.stripWhitespace("""
            {
              "template": {
                "id": ".attack-discovery",
                "version": 2,
                "variables": { "time_range": "24h", "alerts": [ { "id": "a1" } ] }
              },
              "temperature": 0.1
            }
            """));

        var requestMap = lastRequestBody(mockEISServer);
        assertThat(requestMap.get(UnifiedCompletionUtils.MESSAGES_FIELD), nullValue());
        assertThat(requestMap.get(UnifiedCompletionUtils.MODEL_FIELD), is(TEMPLATE_REQUIRED_MODEL));
        assertThat(requestMap.get(UnifiedCompletionUtils.TEMPERATURE_FIELD), is(0.1));

        var template = (Map<String, Object>) requestMap.get(UnifiedCompletionUtils.TEMPLATE_FIELD);
        assertThat(template, not(nullValue()));
        assertThat(template.get(UnifiedCompletionUtils.TEMPLATE_ID_FIELD), is(".attack-discovery"));
        assertThat(template.get(UnifiedCompletionUtils.TEMPLATE_VERSION_FIELD), is(2));
        assertThat(
            template.get(UnifiedCompletionUtils.TEMPLATE_VARIABLES_FIELD),
            is(Map.of("time_range", "24h", "alerts", List.of(Map.of("id", "a1"))))
        );
    }
}
