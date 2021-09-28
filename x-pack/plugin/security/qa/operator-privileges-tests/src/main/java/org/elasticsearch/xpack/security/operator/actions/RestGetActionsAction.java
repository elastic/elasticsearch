/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator.actions;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetActionsAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_test/get_actions"));
    }

    @Override
    public String getName() {
        return "test_get_actions";
    }

    @SuppressForbidden(reason = "Use reflection for testing only")
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final Map<ActionType, TransportAction> actions = AccessController.doPrivileged(
            (PrivilegedAction<Map<ActionType, TransportAction>>) () -> {
                try {
                    final Field actionsField = client.getClass().getDeclaredField("actions");
                    actionsField.setAccessible(true);
                    return (Map<ActionType, TransportAction>) actionsField.get(client);
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw new ElasticsearchException(e);
                }
            }
        );

        final List<String> actionNames = actions.keySet().stream().map(ActionType::name).collect(Collectors.toList());
        return channel -> new RestToXContentListener<>(channel).onResponse(
            (builder, params) -> builder.startObject().field("actions", actionNames).endObject()
        );
    }
}
