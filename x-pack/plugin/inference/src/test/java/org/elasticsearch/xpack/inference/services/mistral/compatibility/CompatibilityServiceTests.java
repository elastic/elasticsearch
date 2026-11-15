/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.compatibility;

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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.InferenceFeatures.INFERENCE_MISTRAL_URL_ADDED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompatibilityServiceTests extends ESTestCase {

    private static final String NODE_ID = "node-1";
    private static final URI CUSTOM_URL = URI.create("https://custom.mistral.example.com/v1/embeddings");

    private static final FeatureService FEATURE_SERVICE = new FeatureService(List.of(new InferenceFeatures()));

    public void testCheckCompatibility_RequestContext_UrlSet_FeatureAbsent_ThrowsBadRequest() {
        var service = createCompatibilityService(false);

        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> service.checkCompatibility(CUSTOM_URL, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), is(CompatibilityService.URL_UNSUPPORTED_MESSAGE));
        assertThat(exception.getMessage(), containsString("[url]"));
    }

    public void testCheckCompatibility_RequestContext_UrlSet_FeaturePresent_DoesNotThrow() {
        var service = createCompatibilityService(true);

        service.checkCompatibility(CUSTOM_URL, ConfigurationParseContext.REQUEST);
    }

    public void testCheckCompatibility_RequestContext_UrlNull_FeatureAbsent_DoesNotThrow() {
        var service = createCompatibilityService(false);

        service.checkCompatibility(null, ConfigurationParseContext.REQUEST);
    }

    public void testCheckCompatibility_RequestContext_UrlNull_FeaturePresent_DoesNotThrow() {
        var service = createCompatibilityService(true);

        service.checkCompatibility(null, ConfigurationParseContext.REQUEST);
    }

    public void testCheckCompatibility_PersistentContext_UrlSet_FeatureAbsent_DoesNotThrow() {
        var service = createCompatibilityService(false);

        service.checkCompatibility(CUSTOM_URL, ConfigurationParseContext.PERSISTENT);
    }

    public void testCheckCompatibility_PersistentContext_DoesNotThrow_ForAnyUrlAndFeatureState() {
        var service = createCompatibilityService(randomBoolean());

        service.checkCompatibility(randomBoolean() ? CUSTOM_URL : null, ConfigurationParseContext.PERSISTENT);
    }

    private static ClusterState clusterState(boolean urlFeatureSupported) {
        var features = urlFeatureSupported ? Set.of(INFERENCE_MISTRAL_URL_ADDED.id()) : Set.<String>of();
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(NODE_ID)).build())
            .nodeFeatures(Map.of(NODE_ID, features))
            .build();
    }

    /**
     * The {@link ClusterService} is mocked because the real one needs a thread pool and a full cluster applier
     * just to hand back a {@link ClusterState}, and the only thing the check reads from it is that state. The
     * {@link ClusterState} and the {@link FeatureService} are real, so the feature lookup itself is not stubbed.
     */
    private static CompatibilityService createCompatibilityService(boolean urlFeatureSupported) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState(urlFeatureSupported));
        return new CompatibilityService(clusterService, FEATURE_SERVICE);
    }
}
