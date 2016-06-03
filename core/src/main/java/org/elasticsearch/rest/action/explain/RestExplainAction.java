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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
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

    private final IndicesQueriesRegistry indicesQueriesRegistry;

    @Inject
    public RestExplainAction(Settings settings, RestController controller, Client client, IndicesQueriesRegistry indicesQueriesRegistry) {
        super(settings, client);
        this.indicesQueriesRegistry = indicesQueriesRegistry;
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
            BytesReference restContent = RestActions.getRestContent(request);
            explainRequest.query(RestActions.getQueryContent(restContent, indicesQueriesRegistry, parseFieldMatcher));
        } else if (queryString != null) {
            QueryBuilder query = RestActions.urlParamsToQueryBuilder(request);
            explainRequest.query(query);
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
        static final String _INDEX = "_index";
        static final String _TYPE = "_type";
        static final String _ID = "_id";
        static final String MATCHED = "matched";
        static final String EXPLANATION = "explanation";
        static final String VALUE = "value";
        static final String DESCRIPTION = "description";
        static final String DETAILS = "details";
        static final String GET = "get";

    }
}
