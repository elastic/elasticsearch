/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceRequestMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.inference.common.InferencePreferences;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;

import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.inference.InferencePlugin.X_ELASTIC_ES_VERSION;

public abstract class ElasticInferenceServiceRequest implements OutboundRequest {

    public static final String X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER = "X-Elastic-Inference-Allowed-Regions";
    public static final String X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER = "X-Elastic-Inference-Allowed-Geos";

    private final ElasticInferenceServiceRequestMetadata metadata;
    private final InferencePreferences preferences;
    protected final CCMAuthenticationApplierFactory.AuthApplier authApplier;

    public ElasticInferenceServiceRequest(
        ElasticInferenceServiceRequestMetadata metadata,
        @Nullable InferencePreferences preferences,
        CCMAuthenticationApplierFactory.AuthApplier authApplier
    ) {
        this.metadata = Objects.requireNonNull(metadata);
        this.preferences = preferences;
        this.authApplier = Objects.requireNonNull(authApplier);
    }

    public ElasticInferenceServiceRequestMetadata getMetadata() {
        return metadata;
    }

    @Nullable
    public InferencePreferences getPreferences() {
        return preferences;
    }

    @Override
    public final void createHttpRequest(ActionListener<HttpRequest> listener) {
        final HttpRequestBase request = createHttpRequestBase();
        // TODO: consider moving tracing here, too

        // addHeader, not setHeader: createHttpRequestBase may already have set X-elastic-product-use-case.
        // Sparse and dense embeddings write the input-type value first; the caller value is appended after it.
        addHeaderIfPresent(request, Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, metadata.productOrigin());
        metadata.context().forEachPresent((field, value) -> addHeaderIfPresent(request, field.httpHeader(), value));
        addHeaderIfPresent(request, X_ELASTIC_ES_VERSION, metadata.esVersion());

        addRegionPolicyHeaders(request, preferences);

        listener.onResponse(new HttpRequest(authApplier.apply(request), getInferenceEntityId()));
    }

    private static void addHeaderIfPresent(HttpRequestBase request, String header, @Nullable String value) {
        if (Strings.isNullOrEmpty(value) == false) {
            request.addHeader(header, value);
        }
    }

    private static void addRegionPolicyHeaders(HttpRequestBase request, @Nullable InferencePreferences preferences) {
        if (preferences == null || preferences.regionPolicy() == null) {
            return;
        }

        var regionPolicy = preferences.regionPolicy();
        var allowedRegions = regionPolicy.allowedRegions();
        var allowedGeos = regionPolicy.allowedGeos();

        if (allowedRegions != null && allowedRegions.isEmpty() == false) {
            var value = allowedRegions.stream().map(r -> r.csp() + ":" + r.region()).collect(Collectors.joining(","));
            request.addHeader(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER, value);
        } else if (allowedGeos != null && allowedGeos.isEmpty() == false) {
            request.addHeader(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER, String.join(",", allowedGeos));
        }
    }

    protected abstract HttpRequestBase createHttpRequestBase();

    public static ElasticInferenceServiceRequestMetadata extractRequestMetadataFromThreadContext(ThreadContext context) {
        return new ElasticInferenceServiceRequestMetadata(
            InferenceRequestMetadata.capture(context::getHeader),
            context.getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER),
            Version.CURRENT.toString()
        );
    }
}
