/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.rest;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.IntConsumer;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestFleetSearchAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "fleet_search_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_fleet/_fleet_search"), new Route(POST, "/{index}/_fleet/_fleet_search"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SearchRequest searchRequest;
        if (request.hasParam("min_compatible_shard_node")) {
            searchRequest = new SearchRequest(Version.fromString(request.param("min_compatible_shard_node")));
        } else {
            searchRequest = new SearchRequest();
        }
        String[] indices = searchRequest.indices();
        if (indices.length > 1) {
            throw new IllegalArgumentException(
                "Fleet search API only supports searching a single index. Found: [" + Arrays.toString(indices) + "]."
            );
        }

        IntConsumer setSize = size -> searchRequest.source().size(size);
        request.withContentOrSourceParamParserOrNull(
            parser -> RestSearchAction.parseSearchRequest(
                searchRequest,
                request,
                parser,
                client.getNamedWriteableRegistry(),
                setSize,
                (restRequest, sr) -> {
                    String[] stringWaitForCheckpoints = request.paramAsStringArray("wait_for_checkpoints", Strings.EMPTY_ARRAY);
                    final long[] waitForCheckpoints = new long[stringWaitForCheckpoints.length];
                    for (int i = 0; i < stringWaitForCheckpoints.length; ++i) {
                        waitForCheckpoints[i] = Long.parseLong(stringWaitForCheckpoints[i]);
                    }
                    String[] indices1 = Strings.splitStringByCommaToArray(request.param("index"));
                    if (indices1.length > 1) {
                        throw new IllegalArgumentException(
                            "Fleet search API only supports searching a single index. Found: [" + Arrays.toString(indices1) + "]."
                        );
                    }
                    if (waitForCheckpoints.length != 0) {
                        sr.setWaitForCheckpoints(Collections.singletonMap(indices1[0], waitForCheckpoints));
                    }
                    final TimeValue waitForCheckpointsTimeout = request.paramAsTime(
                        "wait_for_checkpoints_timeout",
                        TimeValue.timeValueSeconds(30)
                    );
                    sr.setWaitForCheckpointsTimeout(waitForCheckpointsTimeout);
                }
            )
        );

        return channel -> {
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(SearchAction.INSTANCE, searchRequest, new RestStatusToXContentListener<>(channel));
        };
    }

    @Override
    protected Set<String> responseParams() {
        return RestSearchAction.RESPONSE_PARAMS;
    }
}
