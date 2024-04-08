/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateRequest;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

/** Rest handler for feature state reset requests */
@ServerlessScope(Scope.INTERNAL)
public class RestResetFeatureStateAction extends BaseRestHandler {

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_features/_reset"));
    }

    @Override
    public String getName() {
        return "reset_feature_state";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final ResetFeatureStateRequest req = new ResetFeatureStateRequest();

        return restChannel -> client.execute(ResetFeatureStateAction.INSTANCE, req, new RestToXContentListener<>(restChannel, r -> {
            long failures = r.getFeatureStateResetStatuses()
                .stream()
                .filter(status -> status.getStatus() == ResetFeatureStateResponse.ResetFeatureStateStatus.Status.FAILURE)
                .count();
            if (failures == 0) {
                return RestStatus.OK;
            } else if (failures == r.getFeatureStateResetStatuses().size()) {
                return RestStatus.INTERNAL_SERVER_ERROR;
            }
            return RestStatus.MULTI_STATUS;
        }));
    }
}
