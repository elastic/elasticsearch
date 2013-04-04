/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.explain.ExplainSourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

/**
 * Rest action for computing a score explanation for specific documents.
 */
public class RestExplainAction extends BaseRestHandler {

    @Inject
    public RestExplainAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_explain", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_explain", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final ExplainRequest explainRequest = new ExplainRequest(request.param("index"), request.param("type"), request.param("id"));
        explainRequest.parent(request.param("parent"));
        explainRequest.routing(request.param("routing"));
        explainRequest.preference(request.param("preference"));
        String sourceString = request.param("source");
        String queryString = request.param("q");
        if (request.hasContent()) {
            explainRequest.source(request.content(), request.contentUnsafe());
        } else if (sourceString != null) {
            explainRequest.source(new BytesArray(request.param("source")), false);
        } else if (queryString != null) {
            QueryStringQueryBuilder queryStringBuilder = QueryBuilders.queryString(queryString);
            queryStringBuilder.defaultField(request.param("df"));
            queryStringBuilder.analyzer(request.param("analyzer"));
            queryStringBuilder.analyzeWildcard(request.paramAsBoolean("analyze_wildcard", false));
            queryStringBuilder.lowercaseExpandedTerms(request.paramAsBoolean("lowercase_expanded_terms", true));
            queryStringBuilder.lenient(request.paramAsBooleanOptional("lenient", null));
            String defaultOperator = request.param("default_operator");
            if (defaultOperator != null) {
                if ("OR".equals(defaultOperator)) {
                    queryStringBuilder.defaultOperator(QueryStringQueryBuilder.Operator.OR);
                } else if ("AND".equals(defaultOperator)) {
                    queryStringBuilder.defaultOperator(QueryStringQueryBuilder.Operator.AND);
                } else {
                    throw new ElasticSearchIllegalArgumentException("Unsupported defaultOperator [" + defaultOperator + "], can either be [OR] or [AND]");
                }
            }

            ExplainSourceBuilder explainSourceBuilder = new ExplainSourceBuilder();
            explainSourceBuilder.setQuery(queryStringBuilder);
            explainRequest.source(explainSourceBuilder);
        }

        String sField = request.param("fields");
        if (sField != null) {
            String[] sFields = Strings.splitStringByCommaToArray(sField);
            if (sFields != null) {
                explainRequest.fields(sFields);
            }
        }

        client.explain(explainRequest, new ActionListener<ExplainResponse>() {

            @Override
            public void onResponse(ExplainResponse response) {
                try {
                    XContentBuilder builder = restContentBuilder(request);
                    builder.startObject();
                    builder.field(Fields.OK, response.isExists())
                            .field(Fields._INDEX, explainRequest.index())
                            .field(Fields._TYPE, explainRequest.type())
                            .field(Fields._ID, explainRequest.id())
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
                    channel.sendResponse(new XContentRestResponse(request, response.isExists() ? OK : NOT_FOUND, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
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

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    static class Fields {
        static final XContentBuilderString OK = new XContentBuilderString("ok");
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
