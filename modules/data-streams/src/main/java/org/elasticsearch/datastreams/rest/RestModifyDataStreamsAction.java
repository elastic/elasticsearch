/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.rest;

import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.datastreams.DataStreamFeatures;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestModifyDataStreamsAction extends BaseRestHandler {

    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestModifyDataStreamsAction(Predicate<NodeFeature> clusterSupportsFeature) {
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public String getName() {
        return "modify_data_stream_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_data_stream/_modify"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ModifyDataStreamsAction.Request modifyDsRequest;
        try (XContentParser parser = request.contentParser()) {
            modifyDsRequest = ModifyDataStreamsAction.Request.PARSER.parse(
                parser,
                actions -> new ModifyDataStreamsAction.Request(
                    RestUtils.getMasterNodeTimeout(request),
                    RestUtils.getAckTimeout(request),
                    actions
                )
            );
        }
        if (modifyDsRequest.getActions() == null || modifyDsRequest.getActions().isEmpty()) {
            throw new IllegalArgumentException("no data stream actions specified, at least one must be specified");
        }
        boolean hasDeleteIndexAction = modifyDsRequest.getActions()
            .stream()
            .anyMatch(action -> action.getType() == DataStreamAction.Type.DELETE_BACKING_INDEX);
        if (hasDeleteIndexAction) {
            if (clusterSupportsFeature.test(DataStreamFeatures.DATA_STREAMS_MODIFY_DELETE_INDEX) == false) {
                throw new IllegalArgumentException("delete_backing_index is an unsupported action type for this mixed version cluster");
            }
        }
        return channel -> client.execute(ModifyDataStreamsAction.INSTANCE, modifyDsRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of("data_streams.modify.delete_index");
    }
}
