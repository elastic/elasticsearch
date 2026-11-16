/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.client.internal.node.NodeClient;
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
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_FORMAT;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_HEADER;

/**
 * REST handler for {@code POST /_query/stream}: runs an ES|QL query and streams the result to the client as NDJSON
 * as it is produced, one row per line. See {@link EsqlStreamResponseListener} for the response shape.
 *
 * <p>The body is the same as for {@code POST /_query}, except that {@code columnar} is not supported. The query runs
 * only for as long as the connection stays open: a disconnect cancels it.</p>
 */
@ServerlessScope(Scope.PUBLIC)
public class RestEsqlStreamQueryAction extends BaseRestHandler {
    private static final Logger LOGGER = LogManager.getLogger(RestEsqlStreamQueryAction.class);

    static final String NDJSON_FORMAT_VALUE = "ndjson";
    static final String NDJSON_MEDIA_TYPE = "application/x-ndjson";

    /**
     * Streaming is unreleased, so it is available on snapshot builds only. When this is false the handler is never
     * registered, which makes the endpoint indistinguishable from one that was never added.
     */
    public static final boolean ENABLED = Build.current().isSnapshot();

    private final EsqlCapabilities capabilities;

    public RestEsqlStreamQueryAction(EsqlCapabilities capabilities) {
        this.capabilities = capabilities;
    }

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
        return capabilities.capabilities();
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        EsqlQueryRequest esqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            esqlRequest = RequestXContent.parseSync(parser);
        }
        return streamingChannelConsumer(esqlRequest, request, client);
    }

    static RestChannelConsumer streamingChannelConsumer(EsqlQueryRequest esqlRequest, RestRequest request, NodeClient client) {
        if (esqlRequest.columnar()) {
            throw new IllegalArgumentException("[columnar] cannot be used with [POST /_query/stream]");
        }
        // format and header are read here, not in the channel consumer: BaseRestHandler rejects any parameter that
        // is still unconsumed once prepareRequest returns.
        validateFormat(request.param(URL_PARAM_FORMAT), request.header("Accept"));
        request.param(URL_PARAM_HEADER);

        final Boolean partialResults = request.paramAsBoolean("allow_partial_results", null);
        if (partialResults != null) {
            esqlRequest.allowPartialResults(partialResults);
        }
        LOGGER.debug("Beginning streaming execution of ESQL query.\nQuery string: [{}]", esqlRequest.queryDescription());

        return channel -> {
            EsqlStreamResponseListener restListener = new EsqlStreamResponseListener(channel, client.threadPool().getThreadContext());
            EsqlStreamQueryRequest streamRequest = new EsqlStreamQueryRequest(
                esqlRequest,
                restListener.resultStreamListener(),
                request.paramAsBoolean(EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION, false),
                restListener::setPreHeaderFailureFooter
            );
            new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
                EsqlStreamQueryAction.INSTANCE,
                streamRequest,
                restListener
            );
        };
    }

    /**
     * NDJSON is the only format for now. It is the default, so a request may leave out both the {@code format} parameter
     * and the {@code Accept} header, but it may not ask for anything else.
     */
    static void validateFormat(String format, String accept) {
        if (format != null && NDJSON_FORMAT_VALUE.equals(format) == false) {
            throw new IllegalArgumentException(
                "[format] must be [" + NDJSON_FORMAT_VALUE + "] for [POST /_query/stream], got [" + format + "]"
            );
        }
        if (accept == null || accept.isBlank()) {
            return;
        }
        for (String mediaRange : accept.split(",")) {
            String mediaType = mediaRange.split(";", 2)[0].trim();
            if (mediaType.equals("*/*") || mediaType.equals("application/*") || mediaType.equals(NDJSON_MEDIA_TYPE)) {
                return;
            }
        }
        throw new IllegalArgumentException(
            "[Accept] must allow [" + NDJSON_MEDIA_TYPE + "] for [POST /_query/stream], got [" + accept + "]"
        );
    }

    @Override
    protected Set<String> responseParams() {
        return Set.of(URL_PARAM_FORMAT, URL_PARAM_HEADER, EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION);
    }
}
