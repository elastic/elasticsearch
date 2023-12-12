/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ShutdownPlugin extends Plugin implements ActionPlugin {
    @Override
    public Collection<?> createComponents(PluginServices services) {

        NodeSeenService nodeSeenService = new NodeSeenService(services.clusterService());

        return Collections.singletonList(nodeSeenService);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        ActionHandler<PutShutdownNodeAction.Request, AcknowledgedResponse> putShutdown = new ActionHandler<>(
            PutShutdownNodeAction.INSTANCE,
            TransportPutShutdownNodeAction.class
        );
        ActionHandler<DeleteShutdownNodeAction.Request, AcknowledgedResponse> deleteShutdown = new ActionHandler<>(
            DeleteShutdownNodeAction.INSTANCE,
            TransportDeleteShutdownNodeAction.class
        );
        ActionHandler<GetShutdownStatusAction.Request, GetShutdownStatusAction.Response> getStatus = new ActionHandler<>(
            GetShutdownStatusAction.INSTANCE,
            TransportGetShutdownStatusAction.class
        );
        return Arrays.asList(putShutdown, deleteShutdown, getStatus);
    }

    @Override
    public List<RestHandler> getRestHandlers(RestHandlerParameters parameters) {
        return Arrays.asList(new RestPutShutdownNodeAction(), new RestDeleteShutdownNodeAction(), new RestGetShutdownStatusAction());
    }
}
