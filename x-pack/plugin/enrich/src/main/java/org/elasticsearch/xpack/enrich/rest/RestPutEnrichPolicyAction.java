/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutEnrichPolicyAction extends BaseRestHandler {

    /**
     * Supplies the current maximum allowed request body size. Read live (rather than captured) so that updates to the backing
     * {@code enrich.max_policy_size} cluster setting take effect without a restart.
     */
    private final Supplier<ByteSizeValue> maxRequestBodySize;

    public RestPutEnrichPolicyAction(Supplier<ByteSizeValue> maxRequestBodySize) {
        this.maxRequestBodySize = maxRequestBodySize;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_enrich/policy/{name}"));
    }

    @Override
    public String getName() {
        return "put_enrich_policy";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) throws IOException {
        // Reject an oversized request body before parsing it. Parsing materializes the whole body on the heap, so a single very large
        // policy could exhaust the heap before any transport-layer validation runs. The content length is known up front from the
        // request, making this an O(1) guard.
        final ByteSizeValue maxSize = maxRequestBodySize.get();
        if (restRequest.contentLength() > maxSize.getBytes()) {
            throw new IllegalArgumentException(
                "Enrich policy request body ["
                    + ByteSizeValue.ofBytes(restRequest.contentLength())
                    + "] exceeds the maximum allowed size of ["
                    + maxSize
                    + "]; this limit is controlled by the [enrich.max_policy_size] setting"
            );
        }
        final PutEnrichPolicyAction.Request request = createRequest(restRequest);
        return channel -> client.execute(PutEnrichPolicyAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    static PutEnrichPolicyAction.Request createRequest(RestRequest restRequest) throws IOException {
        try (XContentParser parser = restRequest.contentOrSourceParamParser()) {
            return PutEnrichPolicyAction.fromXContent(RestUtils.getMasterNodeTimeout(restRequest), parser, restRequest.param("name"));
        }
    }
}
