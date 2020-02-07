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

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Rest action for computing a score explanation for specific documents.
 */
public class RestExplainAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/{index}/_explain/{id}"),
            new Route(POST, "/{index}/_explain/{id}")));
    }

    @Override
    public String getName() {
        return "explain_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ExplainRequest explainRequest = new ExplainRequest(request.param("index"), request.param("id"));

        explainRequest.parent(request.param("parent"));
        explainRequest.routing(request.param("routing"));
        explainRequest.preference(request.param("preference"));
        String queryString = request.param("q");
        request.withContentOrSourceParamParserOrNull(parser -> {
            if (parser != null) {
                explainRequest.query(RestActions.getQueryContent(parser));
            } else if (queryString != null) {
                QueryBuilder query = RestActions.urlParamsToQueryBuilder(request);
                explainRequest.query(query);
            }
        });

        if (request.param("fields") != null) {
            throw new IllegalArgumentException("The parameter [fields] is no longer supported, " +
                "please use [stored_fields] to retrieve stored fields");
        }
        String sField = request.param("stored_fields");
        if (sField != null) {
            String[] sFields = Strings.splitStringByCommaToArray(sField);
            if (sFields != null) {
                explainRequest.storedFields(sFields);
            }
        }

        explainRequest.fetchSourceContext(FetchSourceContext.parseFromRestRequest(request));

        return channel -> client.explain(explainRequest, new RestStatusToXContentListener<>(channel));
    }
}
