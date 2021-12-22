/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.rest;

import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestFleetMultiSearchAction extends BaseRestHandler {

    private final boolean allowExplicitIndex;

    public RestFleetMultiSearchAction(Settings settings) {
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public String getName() {
        return "fleet_msearch_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_fleet/_fleet_msearch"),
            new Route(POST, "/_fleet/_fleet_msearch"),
            new Route(GET, "/{index}/_fleet/_fleet_msearch"),
            new Route(POST, "/{index}/_fleet/_fleet_msearch")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final MultiSearchRequest multiSearchRequest = RestMultiSearchAction.parseRequest(
            request,
            client.getNamedWriteableRegistry(),
            allowExplicitIndex,
            (key, value, searchRequest) -> {
                if ("wait_for_checkpoints".equals(key)) {
                    String[] stringWaitForCheckpoints = nodeStringArrayValue(value);
                    final long[] waitForCheckpoints = new long[stringWaitForCheckpoints.length];
                    for (int i = 0; i < stringWaitForCheckpoints.length; ++i) {
                        waitForCheckpoints[i] = Long.parseLong(stringWaitForCheckpoints[i]);
                    }
                    if (waitForCheckpoints.length != 0) {
                        searchRequest.setWaitForCheckpoints(Collections.singletonMap("*", waitForCheckpoints));
                    }
                    return true;
                } else if ("wait_for_checkpoints_timeout".equals(key)) {
                    final TimeValue waitForCheckpointsTimeout = nodeTimeValue(value, TimeValue.timeValueSeconds(30));
                    searchRequest.setWaitForCheckpointsTimeout(waitForCheckpointsTimeout);
                    return true;
                } else {
                    return false;
                }
            }
        );

        for (SearchRequest searchRequest : multiSearchRequest.requests()) {
            String[] indices = searchRequest.indices();
            Map<String, long[]> waitForCheckpoints = searchRequest.getWaitForCheckpoints();
            if (waitForCheckpoints.isEmpty() == false) {
                if (indices.length == 0) {
                    throw new IllegalArgumentException(
                        "Fleet search API param wait_for_checkpoints is only supported with an index to search specified. "
                            + "No index specified."
                    );
                } else if (indices.length > 1) {
                    throw new IllegalArgumentException(
                        "Fleet search API only supports searching a single index. Found: [" + Arrays.toString(indices) + "]."
                    );
                }
            }
            long[] checkpoints = searchRequest.getWaitForCheckpoints().get("*");
            if (checkpoints != null) {
                searchRequest.setWaitForCheckpoints(Collections.singletonMap(indices[0], checkpoints));
            }
        }

        return channel -> {
            final RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(MultiSearchAction.INSTANCE, multiSearchRequest, new RestToXContentListener<>(channel));
        };
    }

    @Override
    protected Set<String> responseParams() {
        return RestSearchAction.RESPONSE_PARAMS;
    }
}
