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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class ShutdownPlugin extends Plugin implements ActionPlugin {
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
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return Arrays.asList(new RestPutShutdownNodeAction(), new RestDeleteShutdownNodeAction(), new RestGetShutdownStatusAction());
    }
}
