/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic;

import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;

public abstract class ElasticInferenceServiceRequest implements Request {

    private final String productOrigin;

    public ElasticInferenceServiceRequest(String productOrigin) {
        this.productOrigin = productOrigin;
    }

    public String getProductOrigin() {
        return productOrigin;
    }

    @Override
    public final HttpRequest createHttpRequest() {
        HttpRequestBase request = createHttpRequestBase();
        // TODO: consider moving tracing here, too
        request.setHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, productOrigin);
        return new HttpRequest(request, getInferenceEntityId());
    }

    protected abstract HttpRequestBase createHttpRequestBase();
}
