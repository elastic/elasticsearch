/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.myprofiler.ProfilerState;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ProfilerData extends BaseRestHandler {
    @Override
    public String getName() {
        return "profiler_data_handler";
    }

    @Override
    public List<Route> routes(){
        return List.of(
            new Route(RestRequest.Method.GET,"/profiler/data")
        );
    }


    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {

        //int count = ProfilerState.getInstance().getQueryCount();
        // int count = 2;
        return channel -> {
            try {
                ConcurrentHashMap<String, AtomicLong> indexquerycount = ProfilerState.getInstance().getIndex_query_count();
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.startArray("indices");
                for (Map.Entry<String, AtomicLong> entry : indexquerycount.entrySet()) {
                    builder.startObject();
                    builder.field("index", entry.getKey());
                    builder.field("search_query_count", entry.getValue().get());
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
                channel.sendResponse(new RestResponse(RestStatus.OK, builder));
            }catch (Exception e){
                channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, "failed"));
            }
        };
    }
}
