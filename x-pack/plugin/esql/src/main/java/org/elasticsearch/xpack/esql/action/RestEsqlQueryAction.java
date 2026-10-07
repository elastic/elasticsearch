/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.formatter.NdjsonFormat;
import org.elasticsearch.xpack.esql.plugin.EsqlMediaTypeParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_DELIMITER;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_HEADER;

@ServerlessScope(Scope.PUBLIC)
public class RestEsqlQueryAction extends BaseRestHandler {
    private static final Logger LOGGER = LogManager.getLogger(RestEsqlQueryAction.class);

    static final String STREAMING_OPTION = "streaming";
    static final String BATCH_SIZE_OPTION = NdjsonFormat.URL_PARAM_BATCH_SIZE;

    /**
     * Streaming and the NDJSON format are unreleased and are released together, so they are available on snapshot builds
     * only and share this one gate. When this is false the {@code streaming} and {@code batch_size} parameters are never
     * consumed and are omitted from {@link #responseParams()}, so {@link BaseRestHandler} rejects them as unrecognized
     * parameters, and {@code EsqlMediaTypeParser} does not resolve {@code format=ndjson}, so it is rejected as an invalid format.
     * Each is then indistinguishable from a feature that was never added.
     */
    public static final boolean STREAMING_ENABLED = Build.current().isSnapshot();

    private final EsqlCapabilities capabilities;

    public RestEsqlQueryAction(EsqlCapabilities capabilities) {
        this.capabilities = capabilities;
    }

    @Override
    public String getName() {
        return "esql_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query"));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return capabilities.capabilities();
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        EsqlQueryRequest esqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            esqlRequest = RequestXContent.parseSync(parser);
        }

        if (STREAMING_ENABLED) {
            boolean streaming = request.paramAsBoolean(STREAMING_OPTION, false);
            boolean hasBatchSize = request.param(BATCH_SIZE_OPTION) != null;

            if (streaming || hasBatchSize) {
                MediaType mediaType = EsqlMediaTypeParser.getResponseMediaType(request, esqlRequest);
                if (streaming && EsqlMediaTypeParser.supportsStreaming(mediaType) == false) {
                    throw new IllegalArgumentException("[" + STREAMING_OPTION + "=true] requires [format=ndjson]");
                }
                if (streaming == false && EsqlMediaTypeParser.hasRecordFraming(mediaType) == false) {
                    throw new IllegalArgumentException(
                        "[" + BATCH_SIZE_OPTION + "] requires [" + STREAMING_OPTION + "=true] or [format=ndjson]"
                    );
                }
                if (hasBatchSize) {
                    // Validate up front so a bad value is rejected before the query runs, whether or not it streams.
                    NdjsonFormat.batchSize(request);
                }
            }

            if (streaming) {
                return streamingChannelConsumer(esqlRequest, request, client);
            }
        }
        return restChannelConsumer(esqlRequest, request, client);
    }

    static RestChannelConsumer streamingChannelConsumer(EsqlQueryRequest esqlRequest, RestRequest request, NodeClient client) {
        if (Boolean.TRUE.equals(esqlRequest.includeCCSMetadata())) {
            throw incompatibleWithStreaming("include_ccs_metadata");
        }
        if (Boolean.TRUE.equals(esqlRequest.includeExecutionMetadata())) {
            throw incompatibleWithStreaming("include_execution_metadata");
        }
        if (request.param(URL_PARAM_DELIMITER) != null) {
            throw incompatibleWithStreaming(URL_PARAM_DELIMITER);
        }
        request.param(URL_PARAM_HEADER);

        int batchSize = NdjsonFormat.batchSize(request);

        final Boolean partialResults = request.paramAsBoolean("allow_partial_results", null);
        if (partialResults != null) {
            esqlRequest.allowPartialResults(partialResults);
        }
        final int resolvedBatchSize = batchSize;
        LOGGER.debug("Beginning streaming execution of ESQL query.\nQuery string: [{}]", esqlRequest.queryDescription());

        return channel -> {
            EsqlStreamResponseListener restListener = new EsqlStreamResponseListener(channel);
            EsqlStreamQueryRequest streamRequest = new EsqlStreamQueryRequest(
                esqlRequest,
                restListener.resultStreamListener(),
                request.paramAsBoolean(EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION, false),
                resolvedBatchSize
            );
            new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
                EsqlStreamQueryAction.INSTANCE,
                streamRequest,
                restListener
            );
        };
    }

    protected static RestChannelConsumer restChannelConsumer(EsqlQueryRequest esqlRequest, RestRequest request, NodeClient client) {
        final Boolean partialResults = request.paramAsBoolean("allow_partial_results", null);
        if (partialResults != null) {
            esqlRequest.allowPartialResults(partialResults);
        }
        LOGGER.debug("Beginning execution of ESQL query.\nQuery string: [{}]", esqlRequest.queryDescription());

        return channel -> {
            RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(
                EsqlQueryAction.INSTANCE,
                esqlRequest,
                new EsqlResponseListener(channel, request, esqlRequest, client.threadPool().getThreadContext()).wrapWithLogging()
            );
        };
    }

    private static IllegalArgumentException incompatibleWithStreaming(String option) {
        return new IllegalArgumentException(Strings.format("[%s] cannot be used with [%s=true]", option, STREAMING_OPTION));
    }

    @Override
    protected Set<String> responseParams() {
        if (STREAMING_ENABLED) {
            return Set.of(URL_PARAM_DELIMITER, EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION, STREAMING_OPTION, BATCH_SIZE_OPTION);
        }
        return Set.of(URL_PARAM_DELIMITER, EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION);
    }
}
