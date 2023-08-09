/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.rest;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetRollupIndexCapsAction extends BaseRestHandler {

    static final ParseField INDEX = new ParseField("index");

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, "/{index}/_rollup/data").replaces(GET, "/{index}/_xpack/rollup/data", RestApiVersion.V_7).build()
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String index = restRequest.param(INDEX.getPreferredName());
        IndicesOptions options = IndicesOptions.fromRequest(restRequest, IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED);
        GetRollupIndexCapsAction.Request request = new GetRollupIndexCapsAction.Request(Strings.splitStringByCommaToArray(index), options);

        return channel -> client.threadPool()
            .executor(ThreadPool.Names.MANAGEMENT)
            .execute(ActionRunnable.wrap(new RestBuilderListener<GetRollupIndexCapsAction.Response>(channel) {
                @Override
                public RestResponse buildResponse(GetRollupIndexCapsAction.Response response, XContentBuilder builder) throws Exception {
                    response.toXContent(builder, channel.request());
                    return new RestResponse(RestStatus.OK, builder);
                }
            }, listener -> client.execute(GetRollupIndexCapsAction.INSTANCE, request, listener)));
    }

    @Override
    public String getName() {
        return "get_rollup_index_caps";
    }

}
