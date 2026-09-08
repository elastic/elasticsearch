/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

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

    static final String INCREMENTAL_EXECUTION_OPTION = "incremental_execution";
    static final String BATCH_SIZE_OPTION = "batch_size";
    static final String NDJSON_FORMAT_VALUE = "ndjson";
    static final int DEFAULT_BATCH_SIZE = 100;
    static final int MAX_BATCH_SIZE = 1000;

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

        boolean incrementalExecution = request.paramAsBoolean(INCREMENTAL_EXECUTION_OPTION, false);
        String batchSizeParam = request.param(BATCH_SIZE_OPTION);
        String format = request.param(URL_PARAM_FORMAT);

        if (batchSizeParam != null && incrementalExecution == false) {
            throw new IllegalArgumentException("[" + BATCH_SIZE_OPTION + "] requires [" + INCREMENTAL_EXECUTION_OPTION + "=true]");
        }
        if (NDJSON_FORMAT_VALUE.equals(format) && incrementalExecution == false) {
            throw new IllegalArgumentException("[format=ndjson] requires [" + INCREMENTAL_EXECUTION_OPTION + "=true]");
        }
        if (incrementalExecution && NDJSON_FORMAT_VALUE.equals(format) == false) {
            throw new IllegalArgumentException("[" + INCREMENTAL_EXECUTION_OPTION + "=true] requires [format=ndjson]");
        }

        if (incrementalExecution) {
            return incrementalChannelConsumer(esqlRequest, request, client, batchSizeParam);
        }
        return restChannelConsumer(esqlRequest, request, client);
    }

    static RestChannelConsumer incrementalChannelConsumer(
        EsqlQueryRequest esqlRequest,
        RestRequest request,
        NodeClient client,
        String batchSizeParam
    ) {
        if (esqlRequest.columnar()) {
            throw incompatibleWithIncrementalExecution("columnar");
        }
        if (esqlRequest.profile()) {
            throw incompatibleWithIncrementalExecution("profile");
        }
        if (Boolean.TRUE.equals(esqlRequest.includeCCSMetadata())) {
            throw incompatibleWithIncrementalExecution("include_ccs_metadata");
        }
        if (Boolean.TRUE.equals(esqlRequest.includeExecutionMetadata())) {
            throw incompatibleWithIncrementalExecution("include_execution_metadata");
        }
        if (request.param(URL_PARAM_DELIMITER) != null) {
            throw incompatibleWithIncrementalExecution(URL_PARAM_DELIMITER);
        }
        request.param(URL_PARAM_HEADER);

        int batchSize = DEFAULT_BATCH_SIZE;
        if (batchSizeParam != null) {
            try {
                batchSize = Integer.parseInt(batchSizeParam);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("[" + BATCH_SIZE_OPTION + "] must be an integer, got [" + batchSizeParam + "]");
            }
            if (batchSize < 1) {
                throw new IllegalArgumentException("[" + BATCH_SIZE_OPTION + "] must be at least 1, got [" + batchSize + "]");
            }
            if (batchSize > MAX_BATCH_SIZE) {
                throw new IllegalArgumentException("[" + BATCH_SIZE_OPTION + "] must be at most 1000, got [" + batchSize + "]");
            }
        }

        final Boolean partialResults = request.paramAsBoolean("allow_partial_results", null);
        if (partialResults != null) {
            esqlRequest.allowPartialResults(partialResults);
        }
        final Boolean partialDslFilter = request.paramAsBoolean("allow_partial_dsl_filter", null);
        if (partialDslFilter != null) {
            esqlRequest.allowPartialDslFilter(partialDslFilter);
        }

        final int resolvedBatchSize = batchSize;
        LOGGER.debug("Beginning incremental execution of ESQL query.\nQuery string: [{}]", esqlRequest.queryDescription());

        return channel -> {
            EsqlStreamResponseListener restListener = new EsqlStreamResponseListener(channel);
            EsqlStreamQueryRequest streamRequest = EsqlStreamQueryRequest.from(
                esqlRequest,
                restListener.streamStartListener(),
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
        final Boolean partialDslFilter = request.paramAsBoolean("allow_partial_dsl_filter", null);
        if (partialDslFilter != null) {
            esqlRequest.allowPartialDslFilter(partialDslFilter);
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

    private static IllegalArgumentException incompatibleWithIncrementalExecution(String option) {
        return new IllegalArgumentException(Strings.format("[%s] cannot be used with [%s=true]", option, INCREMENTAL_EXECUTION_OPTION));
    }

    @Override
    protected Set<String> responseParams() {
        return Set.of(URL_PARAM_DELIMITER, EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION, INCREMENTAL_EXECUTION_OPTION, BATCH_SIZE_OPTION);
    }
}
