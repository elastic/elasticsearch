/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Rest action for computing a score explanation for specific documents.
 */
@ServerlessScope(value = Scope.PUBLIC)
public class RestExplainAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_explain/{id}"), new Route(POST, "/{index}/_explain/{id}"));
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
            throw new IllegalArgumentException(
                "The parameter [fields] is no longer supported, please use [stored_fields] to retrieve stored fields"
            );
        }
        String sField = request.param("stored_fields");
        if (sField != null) {
            String[] sFields = Strings.splitStringByCommaToArray(sField);
            if (sFields != null) {
                explainRequest.storedFields(sFields);
            }
        }

        explainRequest.fetchSourceContext(FetchSourceContext.parseFromRestRequest(request));

        return channel -> client.explain(explainRequest, new RestToXContentListener<>(channel, ExplainResponse::status));
    }
}
