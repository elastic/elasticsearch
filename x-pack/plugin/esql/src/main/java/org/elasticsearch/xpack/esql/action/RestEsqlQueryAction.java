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
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_DELIMITER;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_FORMAT;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_HEADER;

@ServerlessScope(Scope.PUBLIC)
public class RestEsqlQueryAction extends BaseRestHandler {
    private static final Logger LOGGER = LogManager.getLogger(RestEsqlQueryAction.class);

    static final String STREAMING_OPTION = "streaming";
    static final String BATCH_SIZE_OPTION = "batch_size";
    static final String NDJSON_FORMAT_VALUE = "ndjson";
    static final int DEFAULT_BATCH_SIZE = 100;
    static final int MAX_BATCH_SIZE = 1000;

    /**
     * Streaming is unreleased, so it is available on snapshot builds only. When this is false the
     * {@code streaming} and {@code batch_size} parameters are never consumed and are omitted from
     * {@link #responseParams()}, so {@link BaseRestHandler} rejects them as unrecognized parameters —
     * making the feature indistinguishable from one that was never added.
     */
    static final boolean STREAMING_ENABLED = Build.current().isSnapshot();

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
            String batchSizeParam = request.param(BATCH_SIZE_OPTION);
            String format = request.param(URL_PARAM_FORMAT);

            if (batchSizeParam != null && streaming == false) {
                throw new IllegalArgumentException("[" + BATCH_SIZE_OPTION + "] requires [" + STREAMING_OPTION + "=true]");
            }
            if (NDJSON_FORMAT_VALUE.equals(format) && streaming == false) {
                throw new IllegalArgumentException("[format=ndjson] requires [" + STREAMING_OPTION + "=true]");
            }
            if (streaming && NDJSON_FORMAT_VALUE.equals(format) == false) {
                throw new IllegalArgumentException("[" + STREAMING_OPTION + "=true] requires [format=ndjson]");
            }

            if (streaming) {
                return streamingChannelConsumer(esqlRequest, request, client, batchSizeParam);
            }
        }
        return restChannelConsumer(esqlRequest, request, client);
    }

    /**
     * Parses and validates the {@code batch_size} parameter. This is the single place {@code batch_size} is
     * validated: nothing downstream re-checks it, so the bounds here are the whole contract. The upper bound
     * is provisional pending the benchmarking in F8.
     */
    static int parseBatchSize(String batchSizeParam) {
        if (batchSizeParam == null) {
            return DEFAULT_BATCH_SIZE;
        }
        int batchSize;
        try {
            batchSize = Integer.parseInt(batchSizeParam);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[" + BATCH_SIZE_OPTION + "] must be an integer, got [" + batchSizeParam + "]");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("[" + BATCH_SIZE_OPTION + "] must be at least 1, got [" + batchSize + "]");
        }
        if (batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                "[" + BATCH_SIZE_OPTION + "] must be at most " + MAX_BATCH_SIZE + ", got [" + batchSize + "]"
            );
        }
        return batchSize;
    }

    static RestChannelConsumer streamingChannelConsumer(
        EsqlQueryRequest esqlRequest,
        RestRequest request,
        NodeClient client,
        String batchSizeParam
    ) {
        if (esqlRequest.columnar()) {
            throw incompatibleWithStreaming("columnar");
        }
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

        final int batchSize = parseBatchSize(batchSizeParam);

        final Boolean partialResults = request.paramAsBoolean("allow_partial_results", null);
        if (partialResults != null) {
            esqlRequest.allowPartialResults(partialResults);
        }
        LOGGER.debug("Beginning streaming execution of ESQL query.\nQuery string: [{}]", esqlRequest.queryDescription());

        return channel -> {
            EsqlStreamResponseListener restListener = new EsqlStreamResponseListener(channel);
            EsqlStreamQueryRequest streamRequest = new EsqlStreamQueryRequest(
                esqlRequest,
                restListener.resultStreamListener(),
                request.paramAsBoolean(EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION, false),
                batchSize
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
                new EsqlResponseListener(channel, request, esqlRequest).wrapWithLogging()
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
