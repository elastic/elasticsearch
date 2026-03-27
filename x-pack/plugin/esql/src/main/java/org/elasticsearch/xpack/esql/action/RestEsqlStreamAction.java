/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.plugin.StreamingProvider;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST handler for streaming ES|QL query results via HTTP chunked transfer encoding.
 * <p>
 * Results are streamed as NDJSON: each line is a JSON object. The first line contains
 * the column metadata, subsequent lines contain page values, and the final line is
 * a completion marker.
 * <p>
 * The query executes with a {@link org.elasticsearch.xpack.esql.plugin.StreamingOutputOperator}
 * that pushes pages directly to the HTTP connection as they are produced, with natural
 * backpressure when the client reads slowly.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestEsqlStreamAction extends BaseRestHandler {
    private static final Logger LOGGER = LogManager.getLogger(RestEsqlStreamAction.class);

    @Override
    public String getName() {
        return "esql_stream_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query/stream"));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return EsqlCapabilities.CAPABILITIES;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        EsqlQueryRequest esqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            esqlRequest = RequestXContent.parseSync(parser);
        }

        if (esqlRequest.pageSize() == null) {
            esqlRequest.pageSize(1000);
        }

        final Boolean partialResults = request.paramAsBoolean("allow_partial_results", null);
        if (partialResults != null) {
            esqlRequest.allowPartialResults(partialResults);
        }
        esqlRequest.streaming(true);

        LOGGER.debug("Beginning streaming execution of ESQL query.\nQuery string: [{}]", esqlRequest.query());

        return channel -> {
            StreamingProvider streamingProvider = new StreamingProvider(channel, esqlRequest.pageSize());
            esqlRequest.streamingProvider(streamingProvider);

            RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(EsqlQueryAction.INSTANCE, esqlRequest, ActionListener.wrap(response -> {
                // The streaming response was already sent via StreamingProvider.
                // The EsqlQueryResponse lifecycle (decRef) is handled by respondAndRelease
                // in TransportEsqlQueryAction.doExecute, so we must not call decRef here.
            }, e -> { streamingProvider.onFailure(e); }));
        };
    }

    @Override
    protected Set<String> responseParams() {
        return Set.of(EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION);
    }
}
