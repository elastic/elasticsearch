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

package org.elasticsearch.rest.action.explain;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * Rest action for computing a score explanation for specific documents.
 */
public class RestExplainAction extends BaseRestHandler {

    @Inject
    public RestExplainAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_explain", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_explain", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final ExplainRequest explainRequest = new ExplainRequest(request.param("index"), request.param("type"), request.param("id"));
        explainRequest.parent(request.param("parent"));
        explainRequest.routing(request.param("routing"));
        explainRequest.preference(request.param("preference"));
        String queryString = request.param("q");
        if (RestActions.hasBodyContent(request)) {
            explainRequest.source(RestActions.getRestContent(request));
        } else if (queryString != null) {
            QueryStringQueryBuilder queryStringBuilder = QueryBuilders.queryStringQuery(queryString);
            queryStringBuilder.defaultField(request.param("df"));
            queryStringBuilder.analyzer(request.param("analyzer"));
            queryStringBuilder.analyzeWildcard(request.paramAsBoolean("analyze_wildcard", false));
            queryStringBuilder.lowercaseExpandedTerms(request.paramAsBoolean("lowercase_expanded_terms", true));
            queryStringBuilder.lenient(request.paramAsBoolean("lenient", null));
            String defaultOperator = request.param("default_operator");
            if (defaultOperator != null) {
                if ("OR".equals(defaultOperator)) {
                    queryStringBuilder.defaultOperator(QueryStringQueryBuilder.Operator.OR);
                } else if ("AND".equals(defaultOperator)) {
                    queryStringBuilder.defaultOperator(QueryStringQueryBuilder.Operator.AND);
                } else {
                    throw new IllegalArgumentException("Unsupported defaultOperator [" + defaultOperator + "], can either be [OR] or [AND]");
                }
            }

            QuerySourceBuilder querySourceBuilder = new QuerySourceBuilder();
            querySourceBuilder.setQuery(queryStringBuilder);
            explainRequest.source(querySourceBuilder);
        }

        String sField = request.param("fields");
        if (sField != null) {
            String[] sFields = Strings.splitStringByCommaToArray(sField);
            if (sFields != null) {
                explainRequest.fields(sFields);
            }
        }

        explainRequest.fetchSourceContext(FetchSourceContext.parseFromRestRequest(request));

        client.explain(explainRequest, new RestBuilderListener<ExplainResponse>(channel) {
            @Override
            public RestResponse buildResponse(ExplainResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(Fields._INDEX, response.getIndex())
                        .field(Fields._TYPE, response.getType())
                        .field(Fields._ID, response.getId())
                        .field(Fields.MATCHED, response.isMatch());

                if (response.hasExplanation()) {
                    builder.startObject(Fields.EXPLANATION);
                    buildExplanation(builder, response.getExplanation());
                    builder.endObject();
                }
                GetResult getResult = response.getGetResult();
                if (getResult != null) {
                    builder.startObject(Fields.GET);
                    response.getGetResult().toXContentEmbedded(builder, request);
                    builder.endObject();
                }
                builder.endObject();
                return new BytesRestResponse(response.isExists() ? OK : NOT_FOUND, builder);
            }

            private void buildExplanation(XContentBuilder builder, Explanation explanation) throws IOException {
                builder.field(Fields.VALUE, explanation.getValue());
                builder.field(Fields.DESCRIPTION, explanation.getDescription());
                Explanation[] innerExps = explanation.getDetails();
                if (innerExps != null) {
                    builder.startArray(Fields.DETAILS);
                    for (Explanation exp : innerExps) {
                        builder.startObject();
                        buildExplanation(builder, exp);
                        builder.endObject();
                    }
                    builder.endArray();
                }
            }
        });
    }

    static class Fields {
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString MATCHED = new XContentBuilderString("matched");
        static final XContentBuilderString EXPLANATION = new XContentBuilderString("explanation");
        static final XContentBuilderString VALUE = new XContentBuilderString("value");
        static final XContentBuilderString DESCRIPTION = new XContentBuilderString("description");
        static final XContentBuilderString DETAILS = new XContentBuilderString("details");
        static final XContentBuilderString GET = new XContentBuilderString("get");

    }
}
