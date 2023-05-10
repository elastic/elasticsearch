/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.validate.query.QueryExplanation;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

@ServerlessScope(Scope.PUBLIC)
public class RestValidateQueryAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestValidateQueryAction.class);
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in validate query requests is deprecated.";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_validate/query"),
            new Route(POST, "/_validate/query"),
            new Route(GET, "/{index}/_validate/query"),
            new Route(POST, "/{index}/_validate/query"),
            Route.builder(GET, "/{index}/{type}/_validate/query").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(POST, "/{index}/{type}/_validate/query").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "validate_query_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("type")) {
            deprecationLogger.compatibleCritical("validate_query_with_types", TYPES_DEPRECATION_MESSAGE);
            request.param("type");
        }

        ValidateQueryRequest validateQueryRequest = new ValidateQueryRequest(Strings.splitStringByCommaToArray(request.param("index")));
        validateQueryRequest.indicesOptions(IndicesOptions.fromRequest(request, validateQueryRequest.indicesOptions()));
        validateQueryRequest.explain(request.paramAsBoolean("explain", false));
        validateQueryRequest.rewrite(request.paramAsBoolean("rewrite", false));
        validateQueryRequest.allShards(request.paramAsBoolean("all_shards", false));

        Exception bodyParsingException = null;
        try {
            request.withContentOrSourceParamParserOrNull(parser -> {
                if (parser != null) {
                    validateQueryRequest.query(RestActions.getQueryContent(parser));
                } else if (request.hasParam("q")) {
                    validateQueryRequest.query(RestActions.urlParamsToQueryBuilder(request));
                }
            });
        } catch (Exception e) {
            bodyParsingException = e;
        }

        final Exception finalBodyParsingException = bodyParsingException;
        return channel -> {
            if (finalBodyParsingException != null) {
                if (finalBodyParsingException instanceof ParsingException) {
                    handleException(validateQueryRequest, ((ParsingException) finalBodyParsingException).getDetailedMessage(), channel);
                } else {
                    handleException(validateQueryRequest, finalBodyParsingException.getMessage(), channel);
                }
            } else {
                client.admin().indices().validateQuery(validateQueryRequest, new RestToXContentListener<>(channel));
            }
        };
    }

    private static void handleException(final ValidateQueryRequest request, final String message, final RestChannel channel)
        throws IOException {
        channel.sendResponse(buildErrorResponse(channel.newBuilder(), message, request.explain()));
    }

    private static RestResponse buildErrorResponse(XContentBuilder builder, String error, boolean explain) throws IOException {
        builder.startObject();
        builder.field(ValidateQueryResponse.VALID_FIELD, false);
        if (explain) {
            builder.field(QueryExplanation.ERROR_FIELD, error);
        }
        builder.endObject();
        return new RestResponse(OK, builder);
    }
}
