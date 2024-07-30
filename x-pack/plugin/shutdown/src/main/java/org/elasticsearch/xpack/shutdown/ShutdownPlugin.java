/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

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
    public List<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return Arrays.asList(new RestPutShutdownNodeAction(), new RestDeleteShutdownNodeAction(), new RestGetShutdownStatusAction());
    }

    @UpdateForV9 // always true in v9 so can be removed
    static boolean serializesWithParentTaskAndTimeouts(TransportVersion transportVersion) {
        return transportVersion.isPatchFrom(TransportVersions.V_8_13_4)
            || transportVersion.isPatchFrom(TransportVersions.V_8_14_0)
            || transportVersion.onOrAfter(TransportVersions.SHUTDOWN_REQUEST_TIMEOUTS_FIX);
    }
}
