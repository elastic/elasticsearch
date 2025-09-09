/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sample;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutSampleConfigAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "put_sample_config";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_sample/{name}/_config"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        TransportPutSampleConfigAction.SamplingConfigCustomMetadata samplingConfig;
        try (XContentParser parser = request.contentParser()) {
            samplingConfig = TransportPutSampleConfigAction.SamplingConfigCustomMetadata.fromXContent(parser);
        }
        PutSampleConfigAction.Request putSampleConfigRequest = new PutSampleConfigAction.Request(
            samplingConfig.rate,
            samplingConfig.maxSamples,
            samplingConfig.maxSize,
            samplingConfig.timeToLive,
            samplingConfig.condition,
            RestUtils.getMasterNodeTimeout(request),
            RestUtils.getAckTimeout(request)
        ).indices(Strings.splitStringByCommaToArray(request.param("name")));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            PutSampleConfigAction.INSTANCE,
            putSampleConfigRequest,
            new RestToXContentListener(channel)
        );
    }

    static class RestToXContentListener extends RestBuilderListener<AcknowledgedResponse> {

        RestToXContentListener(RestChannel channel) {
            super(channel);
        }

        @Override
        public RestResponse buildResponse(AcknowledgedResponse response, XContentBuilder builder) throws Exception {
            response.toXContent(builder, channel.request());
            return new RestResponse(RestStatus.OK, builder);
        }
    }
}
