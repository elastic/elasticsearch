/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestAddVotingConfigExclusionAction extends BaseRestHandler {
    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(30L);

    @Override
    public String getName() {
        return "add_voting_config_exclusions_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_cluster/voting_config_exclusions"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        AddVotingConfigExclusionsRequest votingConfigExclusionsRequest = resolveVotingConfigExclusionsRequest(request);
        return channel -> client.execute(
            AddVotingConfigExclusionsAction.INSTANCE,
            votingConfigExclusionsRequest,
            new RestToXContentListener<>(channel)
        );
    }

    AddVotingConfigExclusionsRequest resolveVotingConfigExclusionsRequest(final RestRequest request) {
        String nodeIds = null;
        String nodeNames = null;

        if (request.hasParam("node_ids")) {
            nodeIds = request.param("node_ids");
        }

        if (request.hasParam("node_names")) {
            nodeNames = request.param("node_names");
        }

        return new AddVotingConfigExclusionsRequest(
            Strings.splitStringByCommaToArray(nodeIds),
            Strings.splitStringByCommaToArray(nodeNames),
            TimeValue.parseTimeValue(request.param("timeout"), DEFAULT_TIMEOUT, getClass().getSimpleName() + ".timeout")
        );
    }

}
