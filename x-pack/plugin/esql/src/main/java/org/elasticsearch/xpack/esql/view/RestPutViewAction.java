/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.View;
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

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutViewAction extends BaseRestHandler {
    private static final String VIEW_INDEX_ABSTRACTION = "view_index_abstraction";
    // Signals that PUT /_query/view/{name} is exposed with @ServerlessScope(Scope.PUBLIC).
    // This capability did not exist before the annotation was added, so old nodes in a mixed
    // cluster will not report it, making /_capabilities?capabilities=views_put_serverless_scope
    // return supported=false for any cluster where any node predates the annotation.
    public static final String VIEWS_PUT_SERVERLESS_SCOPE = "views_put_serverless_scope";
    public static final String VIEW_DESCRIPTION = "view_description";
    public static final String VIEWS_PUT_VALIDATE = "views_put_validate";

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_query/view/{name}"));
    }

    @Override
    public String getName() {
        return "esql_put_view";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            var view = View.parser(request.param("name")).parse(parser, null);
            assert view.isReserved() == false : "Reserved view can not be created or updated via API";
            PutViewAction.Request req = new PutViewAction.Request(
                RestUtils.getMasterNodeTimeout(request),
                RestUtils.getAckTimeout(request),
                view
            );
            boolean validate = request.paramAsBoolean("validate", false);
            return channel -> SubscribableListener.<Void>newForked(l -> {
                if (validate) {
                    // validation runs on current node, actual update below performed on elected master only
                    client.execute(ValidateViewAction.INSTANCE, new ValidateViewAction.Request(view), l.map(ignored -> null));
                } else {
                    l.onResponse(null);
                }
            })
                .<AcknowledgedResponse>andThen((l, ignored) -> client.execute(PutViewAction.INSTANCE, req, l))
                .addListener(new RestToXContentListener<>(channel));
        }
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(VIEW_INDEX_ABSTRACTION, VIEWS_PUT_SERVERLESS_SCOPE, VIEW_DESCRIPTION, VIEWS_PUT_VALIDATE);
    }
}
