/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST endpoint for cursor-aware ESQL autocomplete: {@code POST /_esql/suggestions}.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestEsqlSuggestionsAction extends BaseRestHandler {

    private static final ParseField QUERY = new ParseField("query");
    private static final ParseField CURSOR = new ParseField("cursor");
    private static final ParseField SIZE = new ParseField("size");
    private static final ParseField INCLUDE_SAMPLE_VALUES = new ParseField("include_sample_values");

    private static final ObjectParser<EsqlSuggestionsRequest, Void> PARSER = objectParser();

    private static ObjectParser<EsqlSuggestionsRequest, Void> objectParser() {
        ObjectParser<EsqlSuggestionsRequest, Void> parser = new ObjectParser<>("esql_suggestions", EsqlSuggestionsRequest::new);
        parser.declareString(EsqlSuggestionsRequest::query, QUERY);
        parser.declareInt(EsqlSuggestionsRequest::cursor, CURSOR);
        parser.declareInt(EsqlSuggestionsRequest::size, SIZE);
        parser.declareBoolean(EsqlSuggestionsRequest::includeSampleValues, INCLUDE_SAMPLE_VALUES);
        return parser;
    }

    @Override
    public String getName() {
        return "esql_suggestions";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_esql/suggestions"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        EsqlSuggestionsRequest suggestionsRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            suggestionsRequest = PARSER.apply(parser, null);
        }
        return channel -> {
            RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(EsqlSuggestionsAction.INSTANCE, suggestionsRequest, new RestToXContentListener<>(channel));
        };
    }
}
