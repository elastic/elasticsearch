/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

/**
 * The REST handler for get index and head index APIs.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestGetIndicesAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}"), new Route(HEAD, "/{index}"));
    }

    @Override
    public String getName() {
        return "get_indices_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final GetIndexRequest getIndexRequest = new GetIndexRequest(RestUtils.getMasterNodeTimeout(request));
        getIndexRequest.indices(indices);
        getIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, getIndexRequest.indicesOptions()));
        RestUtils.consumeDeprecatedLocalParameter(request);
        getIndexRequest.humanReadable(request.paramAsBoolean("human", false));
        getIndexRequest.includeDefaults(request.paramAsBoolean("include_defaults", false));
        getIndexRequest.features(GetIndexRequest.Feature.fromRequest(request));
        final var httpChannel = request.getHttpChannel();
        return channel -> new RestCancellableNodeClient(client, httpChannel).admin()
            .indices()
            .getIndex(getIndexRequest, new RestRefCountedChunkedToXContentListener<>(channel));
    }

    /**
     * Parameters used for controlling the response and thus might not be consumed during
     * preparation of the request execution in {@link BaseRestHandler#prepareRequest(RestRequest, NodeClient)}.
     */
    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }
}
