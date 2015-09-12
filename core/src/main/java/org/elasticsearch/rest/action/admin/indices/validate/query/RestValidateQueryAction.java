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

package org.elasticsearch.rest.action.admin.indices.validate.query;

import org.elasticsearch.action.admin.indices.validate.query.QueryExplanation;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;

/**
 *
 */
public class RestValidateQueryAction extends BaseRestHandler {

    @Inject
    public RestValidateQueryAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_validate/query", this);
        controller.registerHandler(POST, "/_validate/query", this);
        controller.registerHandler(GET, "/{index}/_validate/query", this);
        controller.registerHandler(POST, "/{index}/_validate/query", this);
        controller.registerHandler(GET, "/{index}/{type}/_validate/query", this);
        controller.registerHandler(POST, "/{index}/{type}/_validate/query", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        ValidateQueryRequest validateQueryRequest = new ValidateQueryRequest(Strings.splitStringByCommaToArray(request.param("index")));
        validateQueryRequest.indicesOptions(IndicesOptions.fromRequest(request, validateQueryRequest.indicesOptions()));
        if (RestActions.hasBodyContent(request)) {
            validateQueryRequest.source(RestActions.getRestContent(request));
        } else {
            QuerySourceBuilder querySourceBuilder = RestActions.parseQuerySource(request);
            if (querySourceBuilder != null) {
                validateQueryRequest.source(querySourceBuilder);
            }
        }
        validateQueryRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
        if (request.paramAsBoolean("explain", false)) {
            validateQueryRequest.explain(true);
        } else {
            validateQueryRequest.explain(false);
        }
        if (request.paramAsBoolean("rewrite", false)) {
            validateQueryRequest.rewrite(true);
        } else {
            validateQueryRequest.rewrite(false);
        }

        client.admin().indices().validateQuery(validateQueryRequest, new RestBuilderListener<ValidateQueryResponse>(channel) {
            @Override
            public RestResponse buildResponse(ValidateQueryResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field("valid", response.isValid());

                buildBroadcastShardsHeader(builder, request, response);

                if (response.getQueryExplanation() != null && !response.getQueryExplanation().isEmpty()) {
                    builder.startArray("explanations");
                    for (QueryExplanation explanation : response.getQueryExplanation()) {
                        builder.startObject();
                        if (explanation.getIndex() != null) {
                            builder.field("index", explanation.getIndex(), XContentBuilder.FieldCaseConversion.NONE);
                        }
                        builder.field("valid", explanation.isValid());
                        if (explanation.getError() != null) {
                            builder.field("error", explanation.getError());
                        }
                        if (explanation.getExplanation() != null) {
                            builder.field("explanation", explanation.getExplanation());
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
}
