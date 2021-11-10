/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.action.admin.indices.RestUpgradeActionDeprecated.UPGRADE_API_DEPRECATION_MESSAGE;

public class RestUpgradeStatusActionDeprecated extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return org.elasticsearch.core.List.of(
            Route.builder(GET, "/_upgrade").deprecated(UPGRADE_API_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(GET, "/{index}/_upgrade").deprecated(UPGRADE_API_DEPRECATION_MESSAGE, RestApiVersion.V_7).build()
        );
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        UpgradeStatusRequest statusRequest = new UpgradeStatusRequest(Strings.splitStringByCommaToArray(request.param("index")));
        statusRequest.indicesOptions(IndicesOptions.fromRequest(request, statusRequest.indicesOptions()));
        return channel -> client.admin().indices().upgradeStatus(statusRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "upgrade_status_action";
    }
}
