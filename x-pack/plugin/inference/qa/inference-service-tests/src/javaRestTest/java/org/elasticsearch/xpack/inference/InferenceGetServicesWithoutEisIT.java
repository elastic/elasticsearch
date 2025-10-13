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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.assertStatusOkOrCreated;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class InferenceGetServicesWithoutEisIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        // This plugin is located in the inference/qa/test-service-plugin package, look for TestInferenceServicePlugin
        .plugin("inference-service-test")
        .user("x_pack_rest_user", "x-pack-test-password")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testGetServicesWithoutTaskType() throws IOException {
        assertThat(allProviders(), not(hasItem("elastic")));
    }

    private List<String> allProviders() throws IOException {
        return providers(getAllServices());
    }

    @SuppressWarnings("unchecked")
    private List<String> providers(List<Object> services) {
        return services.stream().map(service -> {
            var serviceConfig = (Map<String, Object>) service;
            return (String) serviceConfig.get("service");
        }).toList();
    }

    public void testGetServicesWithTextEmbeddingTaskType() throws IOException {
        var providers = providersFor(TaskType.TEXT_EMBEDDING);
        assertThat(providers.size(), not(equalTo(0)));
        assertThat(providers, not(hasItem("elastic")));
    }

    private List<String> providersFor(TaskType taskType) throws IOException {
        return providers(getServices(taskType));
    }

    public void testGetServicesWithRerankTaskType() throws IOException {
        var providers = providersFor(TaskType.RERANK);
        assertThat(providers.size(), not(equalTo(0)));
        assertThat(providersFor(TaskType.RERANK), not(hasItem("elastic")));
    }

    public void testGetServicesWithCompletionTaskType() throws IOException {
        var providers = providersFor(TaskType.COMPLETION);
        assertThat(providers.size(), not(equalTo(0)));
        assertThat(providersFor(TaskType.COMPLETION), not(hasItem("elastic")));
    }

    public void testGetServicesWithChatCompletionTaskType() throws IOException {
        var providers = providersFor(TaskType.CHAT_COMPLETION);
        assertThat(providers.size(), not(equalTo(0)));
        assertThat(providersFor(TaskType.CHAT_COMPLETION), not(hasItem("elastic")));
    }

    public void testGetServicesWithSparseEmbeddingTaskType() throws IOException {
        var providers = providersFor(TaskType.SPARSE_EMBEDDING);
        assertThat(providers.size(), not(equalTo(0)));
        assertThat(providersFor(TaskType.SPARSE_EMBEDDING), not(hasItem("elastic")));
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
