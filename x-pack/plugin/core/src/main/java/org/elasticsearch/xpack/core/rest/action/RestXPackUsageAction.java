/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rest.action;

import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageRequestBuilder;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

@ServerlessScope(Scope.INTERNAL)
public class RestXPackUsageAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_xpack/usage"));
    }

    @Override
    public String getName() {
        return "xpack_usage_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final TimeValue masterTimeout = request.paramAsTime("master_timeout", MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT);
        final HttpChannel httpChannel = request.getHttpChannel();
        return channel -> new XPackUsageRequestBuilder(new RestCancellableNodeClient(client, httpChannel)).setMasterNodeTimeout(
            masterTimeout
        ).execute(new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(XPackUsageResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                for (XPackFeatureSet.Usage usage : response.getUsages()) {
                    builder.field(usage.name(), usage);
                }
                builder.endObject();
                return new RestResponse(OK, builder);
            }
        });
    }
}
