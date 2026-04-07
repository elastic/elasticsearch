/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.synonyms;

import org.elasticsearch.action.synonyms.DeleteSynonymRuleAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

@ServerlessScope(Scope.PUBLIC)
public class RestDeleteSynonymRuleAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "synonyms_rules_delete_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_synonyms/{synonymsSet}/{synonymRuleId}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        DeleteSynonymRuleAction.Request request = new DeleteSynonymRuleAction.Request(
            restRequest.param("synonymsSet"),
            restRequest.param("synonymRuleId"),
            restRequest.paramAsBoolean("refresh", true)
        );
        return channel -> client.execute(DeleteSynonymRuleAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return SynonymCapabilities.CAPABILITIES;
    }
}
