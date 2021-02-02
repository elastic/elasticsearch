/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestUpgradeActionDeprecated extends BaseRestHandler {

    public static final String UPGRADE_API_DEPRECATION_MESSAGE =
        "The _upgrade API is no longer useful and will be removed. Instead, see _reindex API.";

    @Override
    public List<DeprecatedRoute> deprecatedRoutes() {
        return org.elasticsearch.common.collect.List.of(
            new DeprecatedRoute(POST, "/_upgrade", UPGRADE_API_DEPRECATION_MESSAGE),
            new DeprecatedRoute(POST, "/{index}/_upgrade", UPGRADE_API_DEPRECATION_MESSAGE));
    }

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public String getName() {
        return "upgrade_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        UpgradeRequest upgradeReq = new UpgradeRequest(Strings.splitStringByCommaToArray(request.param("index")));
        upgradeReq.indicesOptions(IndicesOptions.fromRequest(request, upgradeReq.indicesOptions()));
        upgradeReq.upgradeOnlyAncientSegments(request.paramAsBoolean("only_ancient_segments", false));
        return channel -> client.admin().indices().upgrade(upgradeReq, new RestToXContentListener<>(channel));
    }
}
