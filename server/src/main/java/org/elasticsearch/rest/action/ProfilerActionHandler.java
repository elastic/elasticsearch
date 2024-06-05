/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.myprofiler.ProfilerScheduler;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ProfilerActionHandler extends BaseRestHandler {

    private final ProfilerScheduler profilerScheduler;

    @Inject
    public ProfilerActionHandler(NodeClient client) {
        ThreadPool threadPool = client.threadPool();
        this.profilerScheduler = new ProfilerScheduler(threadPool, client, new TimeValue(5, TimeUnit.MINUTES));
    }

    @Override
    public String getName() {
        return "profiler_action_handler";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST,"/profiler/{action}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String action = request.param("action");
        if ("start".equals(action)) {
            profilerScheduler.start();
            return channel -> channel.sendResponse(new RestResponse(RestStatus.OK, "Profiler started"));
        } else if ("stop".equals(action)) {
            profilerScheduler.stop();
            return channel -> channel.sendResponse(new RestResponse(RestStatus.OK, "Profiler stopped"));
        } else {
            return channel -> channel.sendResponse(new RestResponse(RestStatus.BAD_REQUEST, "Invalid action"));
        }
    }

}
