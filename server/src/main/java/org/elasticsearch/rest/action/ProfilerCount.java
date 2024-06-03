/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.myprofiler.ProfilerState;
import org.elasticsearch.rest.BaseRestHandler;

import org.elasticsearch.client.internal.node.NodeClient;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.List;

public class ProfilerCount extends BaseRestHandler {
    @Override
    public String getName() {
        return "profiler_count_handler";
    }

    @Override
    public List<Route> routes(){
        return List.of(
            new Route(RestRequest.Method.GET,"/profiler/count")
        );
    }


    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {

         int count = ProfilerState.getInstance().getQueryCount();
       // int count = 2;
        return channel -> channel.sendResponse(new RestResponse(RestStatus.OK,Integer.toString(count)));
    }
}
