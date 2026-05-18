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
import org.elasticsearch.inference.telemetry.InferenceProductContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;

import java.util.Objects;

import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER;
import static org.elasticsearch.xpack.inference.InferencePlugin.X_ELASTIC_ES_VERSION;

public abstract class ElasticInferenceServiceRequest implements OutboundRequest {

    private final ElasticInferenceServiceRequestMetadata metadata;
    protected final CCMAuthenticationApplierFactory.AuthApplier authApplier;

    public ElasticInferenceServiceRequest(
        ElasticInferenceServiceRequestMetadata metadata,
        CCMAuthenticationApplierFactory.AuthApplier authApplier
    ) {
        this.metadata = Objects.requireNonNull(metadata);
        this.authApplier = Objects.requireNonNull(authApplier);
    }

    public ElasticInferenceServiceRequestMetadata getMetadata() {
        return metadata;
    }

    @Override
    public final void createHttpRequest(ActionListener<HttpRequest> listener) {
        HttpRequestBase request = createHttpRequestBase();
        // TODO: consider moving tracing here, too

        var productOrigin = metadata.context().productOrigin();
        var productUseCase = metadata.context().productUseCase();
        var esVersion = metadata.esVersion();

        if (Strings.isNullOrEmpty(productOrigin) == false) {
            request.setHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, productOrigin);
        }

        if (Strings.isNullOrEmpty(productUseCase) == false) {
            request.addHeader(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, productUseCase);
        }

        if (Strings.isNullOrEmpty(esVersion) == false) {
            request.addHeader(X_ELASTIC_ES_VERSION, esVersion);
        }

        request = authApplier.apply(request);

        listener.onResponse(new HttpRequest(request, getInferenceEntityId()));
    }

    protected abstract HttpRequestBase createHttpRequestBase();

    public static ElasticInferenceServiceRequestMetadata extractRequestMetadataFromThreadContext(ThreadContext context) {
        var inferenceProductContext = InferenceProductContext.create(context);
        return new ElasticInferenceServiceRequestMetadata(inferenceProductContext, Version.CURRENT.toString());
    }
}
