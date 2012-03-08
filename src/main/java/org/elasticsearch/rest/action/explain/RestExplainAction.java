/*
 * Licensed to Nicolas Lalevee under one
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

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

import java.io.IOException;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.search.builder.ExplainSourceBuilder;

/**
 *
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
        ExplainRequest explainRequest = new ExplainRequest(request.param("index"), request.param("type"),
                request.param("id"));
        // no need to have a threaded listener since we just send back a response
        explainRequest.listenerThreaded(false);
        // if we have a local operation, execute it on a thread since we don't spawn
        explainRequest.operationThreaded(true);
        // get the content, and put it in the body
        if (request.hasContent()) {
            explainRequest.source(request.contentByteArray(), request.contentByteArrayOffset(),
                    request.contentLength(), request.contentUnsafe());
        } else {
            String source = request.param("source");
            if (source != null) {
                explainRequest.source(source);
            }
        }
        // add extra source based on the request parameters
        explainRequest.extraSource(parseExplainSource(request));

        explainRequest.routing(request.param("routing"));
        explainRequest.preference(request.param("preference"));

        client.explain(explainRequest, new ActionListener<ExplainResponse>() {
            @Override
            public void onResponse(ExplainResponse response) {
                try {
                    XContentBuilder builder = restContentBuilder(request);
                    response.toXContent(builder, request);
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
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

    private ExplainSourceBuilder parseExplainSource(RestRequest request) {
        ExplainSourceBuilder explainSourceBuilder = null;
        String queryString = request.param("q");
        if (queryString != null) {
            QueryStringQueryBuilder queryBuilder = QueryBuilders.queryString(queryString);
            queryBuilder.defaultField(request.param("df"));
            queryBuilder.analyzer(request.param("analyzer"));
            queryBuilder.analyzeWildcard(request.paramAsBoolean("analyze_wildcard", false));
            queryBuilder.lowercaseExpandedTerms(request.paramAsBoolean("lowercase_expanded_terms", true));
            String defaultOperator = request.param("default_operator");
            if (defaultOperator != null) {
                if ("OR".equals(defaultOperator)) {
                    queryBuilder.defaultOperator(QueryStringQueryBuilder.Operator.OR);
                } else if ("AND".equals(defaultOperator)) {
                    queryBuilder.defaultOperator(QueryStringQueryBuilder.Operator.AND);
                } else {
                    throw new ElasticSearchIllegalArgumentException("Unsupported defaultOperator [" + defaultOperator
                            + "], can either be [OR] or [AND]");
                }
            }
            if (explainSourceBuilder == null) {
                explainSourceBuilder = new ExplainSourceBuilder();
            }
            explainSourceBuilder.query(queryBuilder);
        }

        if (request.hasParam("timeout")) {
            if (explainSourceBuilder == null) {
                explainSourceBuilder = new ExplainSourceBuilder();
            }
            explainSourceBuilder.timeout(request.paramAsTime("timeout", null));
        }

        return explainSourceBuilder;
    }
}
