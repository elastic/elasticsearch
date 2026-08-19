/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.compatibility;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionServiceSettings;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.InferenceFeatures.INFERENCE_ANTHROPIC_COMPLETION_URL_ADDED;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompatibilityServiceTests extends ESTestCase {

    private static final String NODE_ID = "node-1";
    private static final String MODEL_ID = "claude-3-opus-20240229";
    private static final URI CUSTOM_URL = URI.create("https://custom.example.com/v1/messages");

    private static final FeatureService FEATURE_SERVICE = new FeatureService(List.of(new InferenceFeatures()));

    private static final AnthropicChatCompletionServiceSettings SETTINGS_WITH_URL = new AnthropicChatCompletionServiceSettings(
        MODEL_ID,
        CUSTOM_URL,
        null
    );
    private static final AnthropicChatCompletionServiceSettings SETTINGS_WITHOUT_URL = new AnthropicChatCompletionServiceSettings(
        MODEL_ID,
        null
    );

    public void testCheckCompatibility_RequestContext_UrlSet_FeatureAbsent_ThrowsBadRequest() {
        var service = createCompatibilityService(false);

        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> service.checkCompatibility(SETTINGS_WITH_URL, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), is(CompatibilityService.URL_UNSUPPORTED_MESSAGE));
    }

    public void testCheckCompatibility_RequestContext_UrlSet_FeaturePresent_DoesNotThrow() {
        var service = createCompatibilityService(true);

        service.checkCompatibility(SETTINGS_WITH_URL, ConfigurationParseContext.REQUEST);
    }

    public void testCheckCompatibility_RequestContext_UrlNull_FeatureAbsent_DoesNotThrow() {
        var service = createCompatibilityService(false);

        service.checkCompatibility(SETTINGS_WITHOUT_URL, ConfigurationParseContext.REQUEST);
    }

    public void testCheckCompatibility_PersistentContext_UrlSet_FeatureAbsent_DoesNotThrow() {
        var service = createCompatibilityService(false);

        service.checkCompatibility(SETTINGS_WITH_URL, ConfigurationParseContext.PERSISTENT);
    }

    private static ClusterState clusterState(boolean urlFeatureSupported) {
        var features = urlFeatureSupported ? Set.of(INFERENCE_ANTHROPIC_COMPLETION_URL_ADDED.id()) : Set.<String>of();
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(NODE_ID)).build())
            .nodeFeatures(Map.of(NODE_ID, features))
            .build();
    }

    private static CompatibilityService createCompatibilityService(boolean urlFeatureSupported) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState(urlFeatureSupported));
        return new CompatibilityService(clusterService, FEATURE_SERVICE);
    }
}
