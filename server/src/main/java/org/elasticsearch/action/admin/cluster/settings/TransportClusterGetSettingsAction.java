/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalClusterStateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportClusterGetSettingsAction extends TransportLocalClusterStateAction<
    ClusterGetSettingsAction.Request,
    ClusterGetSettingsAction.Response> {

    private final SettingsFilter settingsFilter;

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.CORE_INFRA)
    @SuppressWarnings("this-escape")
    @Inject
    public TransportClusterGetSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        SettingsFilter settingsFilter,
        ActionFilters actionFilters
    ) {
        super(
            ClusterGetSettingsAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.settingsFilter = settingsFilter;

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            ClusterGetSettingsAction.Request::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        ClusterGetSettingsAction.Request request,
        ClusterState state,
        ActionListener<ClusterGetSettingsAction.Response> listener
    ) throws Exception {
        ((CancellableTask) task).ensureNotCancelled();
        Metadata metadata = state.metadata();
        listener.onResponse(
            new ClusterGetSettingsAction.Response(
                settingsFilter.filter(metadata.persistentSettings()),
                settingsFilter.filter(metadata.transientSettings()),
                settingsFilter.filter(metadata.settings())
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterGetSettingsAction.Request request, ClusterState state) {
        return null;
    }
}
