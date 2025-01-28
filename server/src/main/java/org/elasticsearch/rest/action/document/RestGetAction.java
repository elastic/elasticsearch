/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.VersionType;
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
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

@ServerlessScope(Scope.PUBLIC)
public class RestGetAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "document_get_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_doc/{id}"), new Route(HEAD, "/{index}/_doc/{id}"));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        GetRequest getRequest = new GetRequest(request.param("index"), request.param("id"));
        getRequest.refresh(request.paramAsBoolean("refresh", getRequest.refresh()));
        getRequest.routing(request.param("routing"));
        getRequest.preference(request.param("preference"));
        getRequest.realtime(request.paramAsBoolean("realtime", getRequest.realtime()));
        if (request.param("fields") != null) {
            throw new IllegalArgumentException(
                "the parameter [fields] is no longer supported, "
                    + "please use [stored_fields] to retrieve stored fields or [_source] to load the field from _source"
            );
        }
        final String fieldsParam = request.param("stored_fields");
        if (fieldsParam != null) {
            final String[] fields = Strings.splitStringByCommaToArray(fieldsParam);
            if (fields != null) {
                getRequest.storedFields(fields);
            }
        }

        getRequest.version(RestActions.parseVersion(request));
        getRequest.versionType(VersionType.fromString(request.param("version_type"), getRequest.versionType()));

        getRequest.fetchSourceContext(FetchSourceContext.parseFromRestRequest(request));
        if (request.paramAsBoolean("force_synthetic_source", false)) {
            getRequest.setForceSyntheticSource(true);
        }

        return channel -> client.get(getRequest, new RestToXContentListener<>(channel, r -> r.isExists() ? OK : NOT_FOUND));
    }

}
