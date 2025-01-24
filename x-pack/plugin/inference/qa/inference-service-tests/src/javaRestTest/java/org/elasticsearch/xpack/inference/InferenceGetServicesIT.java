/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file has been contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceFeature;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.assertStatusOkOrCreated;
import static org.hamcrest.Matchers.equalTo;

public class InferenceGetServicesIT extends ESRestTestCase {

    // The reason we're retrying is there's a race condition between the node retrieving the
    // authorization response and running the test. Retrieving the authorization should be very fast since
    // we're hosting a local mock server but it's possible it could respond slower. So in the even of a test failure
    // we'll automatically retry after waiting a second.
    @Rule
    public RetryRule retry = new RetryRule(3, TimeValue.timeValueSeconds(1));

    private static final MockElasticInferenceServiceAuthorizationServer mockEISServer = MockElasticInferenceServiceAuthorizationServer
        .enabledWithSparseEmbeddingsAndChatCompletion();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        // Adding both settings unless one feature flag is disabled in a particular environment
        .setting("xpack.inference.elastic.url", mockEISServer::getUrl)
        // TODO remove this once we've removed DEPRECATED_ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG and EIS_GATEWAY_URL
        .setting("xpack.inference.eis.gateway.url", mockEISServer::getUrl)
        // This plugin is located in the inference/qa/test-service-plugin package, look for TestInferenceServicePlugin
        .plugin("inference-service-test")
        .user("x_pack_rest_user", "x-pack-test-password")
        .feature(FeatureFlag.INFERENCE_UNIFIED_API_ENABLED)
        .build();

    // The reason we're doing this is to make sure the mock server is initialized first so we can get the address before communicating
    // it to the cluster as a setting.
    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(mockEISServer).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @SuppressWarnings("unchecked")
    public void testGetServicesWithoutTaskType() throws IOException {
        List<Object> services = getAllServices();
        if ((ElasticInferenceServiceFeature.DEPRECATED_ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled()
            || ElasticInferenceServiceFeature.ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled())) {
            assertThat(services.size(), equalTo(19));
        } else {
            assertThat(services.size(), equalTo(18));
        }

        String[] providers = new String[services.size()];
        for (int i = 0; i < services.size(); i++) {
            Map<String, Object> serviceConfig = (Map<String, Object>) services.get(i);
            providers[i] = (String) serviceConfig.get("service");
        }

        var providerList = new ArrayList<>(
            Arrays.asList(
                "alibabacloud-ai-search",
                "amazonbedrock",
                "anthropic",
                "azureaistudio",
                "azureopenai",
                "cohere",
                "elasticsearch",
                "googleaistudio",
                "googlevertexai",
                "hugging_face",
                "jinaai",
                "mistral",
                "openai",
                "streaming_completion_test_service",
                "test_reranking_service",
                "test_service",
                "text_embedding_test_service",
                "watsonxai"
            )
        );
        if ((ElasticInferenceServiceFeature.DEPRECATED_ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled()
            || ElasticInferenceServiceFeature.ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled())) {
            providerList.add(6, "elastic");
        }
        assertArrayEquals(providerList.toArray(), providers);
    }

    @SuppressWarnings("unchecked")
    public void testGetServicesWithTextEmbeddingTaskType() throws IOException {
        List<Object> services = getServices(TaskType.TEXT_EMBEDDING);
        assertThat(services.size(), equalTo(14));

        String[] providers = new String[services.size()];
        for (int i = 0; i < services.size(); i++) {
            Map<String, Object> serviceConfig = (Map<String, Object>) services.get(i);
            providers[i] = (String) serviceConfig.get("service");
        }

        assertArrayEquals(
            List.of(
                "alibabacloud-ai-search",
                "amazonbedrock",
                "azureaistudio",
                "azureopenai",
                "cohere",
                "elasticsearch",
                "googleaistudio",
                "googlevertexai",
                "hugging_face",
                "jinaai",
                "mistral",
                "openai",
                "text_embedding_test_service",
                "watsonxai"
            ).toArray(),
            providers
        );
    }

    @SuppressWarnings("unchecked")
    public void testGetServicesWithRerankTaskType() throws IOException {
        List<Object> services = getServices(TaskType.RERANK);
        assertThat(services.size(), equalTo(6));

        String[] providers = new String[services.size()];
        for (int i = 0; i < services.size(); i++) {
            Map<String, Object> serviceConfig = (Map<String, Object>) services.get(i);
            providers[i] = (String) serviceConfig.get("service");
        }

        assertArrayEquals(
            List.of("alibabacloud-ai-search", "cohere", "elasticsearch", "googlevertexai", "jinaai", "test_reranking_service").toArray(),
            providers
        );
    }

    @SuppressWarnings("unchecked")
    public void testGetServicesWithCompletionTaskType() throws IOException {
        List<Object> services = getServices(TaskType.COMPLETION);
        assertThat(services.size(), equalTo(9));

        String[] providers = new String[services.size()];
        for (int i = 0; i < services.size(); i++) {
            Map<String, Object> serviceConfig = (Map<String, Object>) services.get(i);
            providers[i] = (String) serviceConfig.get("service");
        }

        var providerList = new ArrayList<>(
            List.of(
                "alibabacloud-ai-search",
                "amazonbedrock",
                "anthropic",
                "azureaistudio",
                "azureopenai",
                "cohere",
                "googleaistudio",
                "openai",
                "streaming_completion_test_service"
            )
        );

        assertArrayEquals(providers, providerList.toArray());
    }

    @SuppressWarnings("unchecked")
    public void testGetServicesWithChatCompletionTaskType() throws IOException {
        List<Object> services = getServices(TaskType.CHAT_COMPLETION);
        if ((ElasticInferenceServiceFeature.DEPRECATED_ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled()
            || ElasticInferenceServiceFeature.ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled())) {
            assertThat(services.size(), equalTo(3));
        } else {
            assertThat(services.size(), equalTo(2));
        }

        String[] providers = new String[services.size()];
        for (int i = 0; i < services.size(); i++) {
            Map<String, Object> serviceConfig = (Map<String, Object>) services.get(i);
            providers[i] = (String) serviceConfig.get("service");
        }

        var providerList = new ArrayList<>(List.of("openai", "streaming_completion_test_service"));

        if ((ElasticInferenceServiceFeature.DEPRECATED_ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled()
            || ElasticInferenceServiceFeature.ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled())) {
            providerList.addFirst("elastic");
        }

        assertArrayEquals(providers, providerList.toArray());
    }

    @SuppressWarnings("unchecked")
    public void testGetServicesWithSparseEmbeddingTaskType() throws IOException {
        List<Object> services = getServices(TaskType.SPARSE_EMBEDDING);

        if ((ElasticInferenceServiceFeature.DEPRECATED_ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled()
            || ElasticInferenceServiceFeature.ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled())) {
            assertThat(services.size(), equalTo(5));
        } else {
            assertThat(services.size(), equalTo(4));
        }

        String[] providers = new String[services.size()];
        for (int i = 0; i < services.size(); i++) {
            Map<String, Object> serviceConfig = (Map<String, Object>) services.get(i);
            providers[i] = (String) serviceConfig.get("service");
        }

        var providerList = new ArrayList<>(Arrays.asList("alibabacloud-ai-search", "elasticsearch", "hugging_face", "test_service"));
        if ((ElasticInferenceServiceFeature.DEPRECATED_ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled()
            || ElasticInferenceServiceFeature.ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled())) {
            providerList.add(1, "elastic");
        }
        assertArrayEquals(providers, providerList.toArray());
    }

    private List<Object> getAllServices() throws IOException {
        var endpoint = Strings.format("_inference/_services");
        return getInternalAsList(endpoint);
    }

    private List<Object> getServices(TaskType taskType) throws IOException {
        var endpoint = Strings.format("_inference/_services/%s", taskType);
        return getInternalAsList(endpoint);
    }

    private List<Object> getInternalAsList(String endpoint) throws IOException {
        var request = new Request("GET", endpoint);
        var response = client().performRequest(request);
        assertStatusOkOrCreated(response);
        return entityAsList(response);
    }
}
