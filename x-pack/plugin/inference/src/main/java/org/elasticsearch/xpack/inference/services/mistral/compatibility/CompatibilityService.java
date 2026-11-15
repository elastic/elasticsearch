/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.compatibility;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.net.URI;
import java.util.Objects;

/**
 * Provides compatibility checks for the Mistral service, rejecting requests that use features
 * not yet supported by all nodes in the cluster.
 */
public class CompatibilityService {

    public static final String URL_UNSUPPORTED_MESSAGE = """
        The field [url] in service_settings is not supported by all nodes in the cluster; \
        please finish upgrading before creating an inference endpoint with a custom url""";

    private final ClusterService clusterService;
    private final FeatureService featureService;

    public CompatibilityService(ClusterService clusterService, FeatureService featureService) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.featureService = Objects.requireNonNull(featureService);
    }

    /**
     * Throws an {@link ElasticsearchStatusException} with status {@code 400} if the caller is
     * creating or updating an endpoint (i.e. the context is {@link ConfigurationParseContext#REQUEST})
     * with a custom {@code url} set and the cluster has not yet been fully upgraded to a version that
     * supports the {@code url} field. A node that does not know the field would ignore it and send
     * requests, including the API key, to the default Mistral endpoint instead.
     *
     * @param url the custom url from the service settings of the endpoint, or {@code null} if the
     *            endpoint uses the default Mistral url. The url is taken directly, rather than a
     *            settings object, because the embeddings and completion settings share no common type.
     * @param context whether the settings come from a user request or from persisted state
     */
    public void checkCompatibility(@Nullable URI url, ConfigurationParseContext context) {
        if (ConfigurationParseContext.isRequestContext(context)
            && url != null
            && featureService.clusterHasFeature(clusterService.state(), InferenceFeatures.INFERENCE_MISTRAL_URL_ADDED) == false) {
            throw new ElasticsearchStatusException(URL_UNSUPPORTED_MESSAGE, RestStatus.BAD_REQUEST);
        }
    }
}
