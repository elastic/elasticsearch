/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

@ServerlessScope(Scope.PUBLIC)
public class RestDeleteComposableIndexTemplateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_index_template/{name}"));
    }

    @Override
    public String getName() {
        return "delete_composable_index_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {

        String[] names = Strings.splitStringByCommaToArray(request.param("name"));
        DeleteComposableIndexTemplateAction.Request deleteReq = new DeleteComposableIndexTemplateAction.Request(names);
        deleteReq.masterNodeTimeout(request.paramAsTime("master_timeout", deleteReq.masterNodeTimeout()));

        return channel -> client.execute(DeleteComposableIndexTemplateAction.INSTANCE, deleteReq, new RestToXContentListener<>(channel));
    }
}
