/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * The REST handler for get source and head source APIs.
 */
public class RestGetSourceAction extends BaseRestHandler {
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestGetSourceAction.class);
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in get_source and exist_source "
        + "requests is deprecated.";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/{index}/_source/{id}"),
            new Route(HEAD, "/{index}/_source/{id}"),
            Route.builder(GET, "/{index}/{type}/{id}/_source")
                .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build(),
            Route.builder(HEAD, "/{index}/{type}/{id}/_source")
                .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build());
    }

    @Override
    public String getName() {
        return "document_get_source_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("type")) {
            request.param("type"); // consume and ignore the type
            deprecationLogger.compatibleApiWarning("get_source_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        final GetRequest getRequest = new GetRequest(request.param("index"), request.param("id"));
        getRequest.refresh(request.paramAsBoolean("refresh", getRequest.refresh()));
        getRequest.routing(request.param("routing"));
        getRequest.preference(request.param("preference"));
        getRequest.realtime(request.paramAsBoolean("realtime", getRequest.realtime()));

        getRequest.fetchSourceContext(FetchSourceContext.parseFromRestRequest(request));

        return channel -> {
            if (getRequest.fetchSourceContext() != null && getRequest.fetchSourceContext().fetchSource() == false) {
                final ActionRequestValidationException validationError = new ActionRequestValidationException();
                validationError.addValidationError("fetching source can not be disabled");
                channel.sendResponse(new BytesRestResponse(channel, validationError));
            } else {
                client.get(getRequest, new RestGetSourceResponseListener(channel, request));
            }
        };
    }

    static class RestGetSourceResponseListener extends RestResponseListener<GetResponse> {
        private final RestRequest request;

        RestGetSourceResponseListener(RestChannel channel, RestRequest request) {
            super(channel);
            this.request = request;
        }

        @Override
        public RestResponse buildResponse(final GetResponse response) throws Exception {
            checkResource(response);

            final XContentBuilder builder = channel.newBuilder(request.getXContentType(), false);
            final BytesReference source = response.getSourceInternal();
            try (InputStream stream = source.streamInput()) {
                builder.rawValue(stream, XContentHelper.xContentType(source));
            }
            return new BytesRestResponse(OK, builder);
        }

        /**
         * Checks if the requested document or source is missing.
         *
         * @param response a response
         * @throws ResourceNotFoundException if the document or source is missing
         */
        private void checkResource(final GetResponse response) {
            final String index = response.getIndex();
            final String id = response.getId();

            if (response.isExists() == false) {
                throw new ResourceNotFoundException("Document not found [" + index + "]/[" + id + "]");
            } else if (response.isSourceEmpty()) {
                throw new ResourceNotFoundException("Source not found [" + index + "]/[" + id + "]");
            }
        }
    }
}
