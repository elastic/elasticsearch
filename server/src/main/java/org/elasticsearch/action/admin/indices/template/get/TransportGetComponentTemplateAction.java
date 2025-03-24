/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

public class TransportGetComponentTemplateAction extends TransportLocalProjectMetadataAction<
    GetComponentTemplateAction.Request,
    GetComponentTemplateAction.Response> {

    private final ClusterSettings clusterSettings;

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @SuppressWarnings("this-escape")
    @Inject
    public TransportGetComponentTemplateAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            GetComponentTemplateAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            projectResolver
        );
        clusterSettings = clusterService.getClusterSettings();

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            GetComponentTemplateAction.Request::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetComponentTemplateAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetComponentTemplateAction.Request request,
        ProjectState state,
        ActionListener<GetComponentTemplateAction.Response> listener
    ) {
        final var cancellableTask = (CancellableTask) task;
        Map<String, ComponentTemplate> allTemplates = state.metadata().componentTemplates();
        Map<String, ComponentTemplate> results;

        // If we did not ask for a specific name, then we return all templates
        if (request.name() == null) {
            results = allTemplates;
        } else {
            results = new HashMap<>();
            String name = request.name();
            if (Regex.isSimpleMatchPattern(name)) {
                for (Map.Entry<String, ComponentTemplate> entry : allTemplates.entrySet()) {
                    if (Regex.simpleMatch(name, entry.getKey())) {
                        results.put(entry.getKey(), entry.getValue());
                    }
                }
            } else if (allTemplates.containsKey(name)) {
                results.put(name, allTemplates.get(name));
            } else {
                throw new ResourceNotFoundException("component template matching [" + request.name() + "] not found");

            }
        }
        cancellableTask.ensureNotCancelled();
        if (request.includeDefaults()) {
            listener.onResponse(
                new GetComponentTemplateAction.Response(
                    results,
                    clusterSettings.get(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING)
                )
            );
        } else {
            listener.onResponse(new GetComponentTemplateAction.Response(results));
        }
    }
}
