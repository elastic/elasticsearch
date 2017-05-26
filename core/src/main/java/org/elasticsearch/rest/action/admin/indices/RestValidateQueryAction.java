/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.validate.query.QueryExplanation;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.RestActions.buildBroadcastShardsHeader;

public class RestValidateQueryAction extends BaseRestHandler {
    public RestValidateQueryAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_validate/query", this);
        controller.registerHandler(POST, "/_validate/query", this);
        controller.registerHandler(GET, "/{index}/_validate/query", this);
        controller.registerHandler(POST, "/{index}/_validate/query", this);
        controller.registerHandler(GET, "/{index}/{type}/_validate/query", this);
        controller.registerHandler(POST, "/{index}/{type}/_validate/query", this);
    }

    @Override
    public String getName() {
        return "validate_query_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ValidateQueryRequest validateQueryRequest = new ValidateQueryRequest(Strings.splitStringByCommaToArray(request.param("index")));
        validateQueryRequest.indicesOptions(IndicesOptions.fromRequest(request, validateQueryRequest.indicesOptions()));
        validateQueryRequest.explain(request.paramAsBoolean("explain", false));
        validateQueryRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
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
                client.admin().indices().validateQuery(validateQueryRequest, new RestBuilderListener<ValidateQueryResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(ValidateQueryResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject();
                        builder.field(VALID_FIELD, response.isValid());
                        buildBroadcastShardsHeader(builder, request, response);
                        if (response.getQueryExplanation() != null && !response.getQueryExplanation().isEmpty()) {
                            builder.startArray(EXPLANATIONS_FIELD);
                            for (QueryExplanation explanation : response.getQueryExplanation()) {
                                builder.startObject();
                                if (explanation.getIndex() != null) {
                                    builder.field(INDEX_FIELD, explanation.getIndex());
                                }
                                if(explanation.getShard() >= 0) {
                                    builder.field(SHARD_FIELD, explanation.getShard());
                                }
                                builder.field(VALID_FIELD, explanation.isValid());
                                if (explanation.getError() != null) {
                                    builder.field(ERROR_FIELD, explanation.getError());
                                }
                                if (explanation.getExplanation() != null) {
                                    builder.field(EXPLANATION_FIELD, explanation.getExplanation());
                                }
                                builder.endObject();
                            }
                            builder.endArray();
                        }
                        builder.endObject();
                        return new BytesRestResponse(OK, builder);
                    }
                });
            }
        };
    }

    private void handleException(final ValidateQueryRequest request, final String message, final RestChannel channel) throws IOException {
        channel.sendResponse(buildErrorResponse(channel.newBuilder(), message, request.explain()));
    }

    private static BytesRestResponse buildErrorResponse(XContentBuilder builder, String error, boolean explain) throws IOException {
        builder.startObject();
        builder.field(VALID_FIELD, false);
        if (explain) {
            builder.field(ERROR_FIELD, error);
        }
        builder.endObject();
        return new BytesRestResponse(OK, builder);
    }

    private static final String INDEX_FIELD = "index";
    private static final String SHARD_FIELD = "shard";
    private static final String VALID_FIELD = "valid";
    private static final String EXPLANATIONS_FIELD = "explanations";
    private static final String ERROR_FIELD = "error";
    private static final String EXPLANATION_FIELD = "explanation";
}
