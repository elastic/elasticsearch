/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.list;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.GetListAction;
import org.elasticsearch.xpack.prelert.lists.ListDocument;

import java.io.IOException;

public class RestGetListAction extends BaseRestHandler {

    private final GetListAction.TransportAction transportGetListAction;

    @Inject
    public RestGetListAction(Settings settings, RestController controller, GetListAction.TransportAction transportGetListAction) {
        super(settings);
        this.transportGetListAction = transportGetListAction;
        controller.registerHandler(RestRequest.Method.GET, PrelertPlugin.BASE_PATH + "lists/{" + ListDocument.ID.getPreferredName() + "}",
                this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        GetListAction.Request getListRequest = new GetListAction.Request(restRequest.param(ListDocument.ID.getPreferredName()));
        return channel -> transportGetListAction.execute(getListRequest, new RestStatusToXContentListener<>(channel));
    }

}
