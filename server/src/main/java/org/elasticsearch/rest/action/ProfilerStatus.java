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

public class ProfilerStatus extends BaseRestHandler {
    @Override
    public String getName() {
        return "profiler_status_handler";
    }

    @Override
    public List<Route> routes(){
        return List.of(
            new Route(RestRequest.Method.GET,"/profiler/status")
        );
    }


    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {

        int status = ProfilerState.getInstance().getStatus();
       // int status=1;
        String a;
        if(status == 1){
            a="profiler is on";
        }
        else{
            a="profiler is off";
        }
        return channel -> channel.sendResponse(new RestResponse(RestStatus.OK,a));
    }
}
