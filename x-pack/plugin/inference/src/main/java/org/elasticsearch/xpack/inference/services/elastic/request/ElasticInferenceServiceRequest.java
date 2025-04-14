/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER;

public abstract class ElasticInferenceServiceRequest implements Request {

    private final ElasticInferenceServiceRequestMetadata metadata;

    public ElasticInferenceServiceRequest(ElasticInferenceServiceRequestMetadata metadata) {
        this.metadata = metadata;
    }

    public ElasticInferenceServiceRequestMetadata getMetadata() {
        return metadata;
    }

    @Override
    public final HttpRequest createHttpRequest() {
        HttpRequestBase request = createHttpRequestBase();
        // TODO: consider moving tracing here, too

        var productOrigin = metadata.productOrigin();
        var productUseCase = metadata.productUseCase();

        if (Objects.nonNull(productOrigin) && productOrigin.isEmpty() == false) {
            request.setHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, metadata.productOrigin());
        }

        if (Objects.nonNull(productUseCase) && productUseCase.isEmpty() == false) {
            request.setHeader(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, metadata.productUseCase());
        }

        return new HttpRequest(request, getInferenceEntityId());
    }

    protected abstract HttpRequestBase createHttpRequestBase();

    public static ElasticInferenceServiceRequestMetadata extractRequestMetadataFromThreadContext(ThreadContext context) {
        // 'X-Elastic-Product-Origin' is an Elastic wide header and therefore present in the ES-wide generic Task class.
        // 'X-Elastic-Product-Use-Case' is Elastic Inference Service specific and is therefore not propagated through the ES-wide Task.
        return new ElasticInferenceServiceRequestMetadata(
            context.getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER),
            context.getHeader(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER)
        );
    }
}
