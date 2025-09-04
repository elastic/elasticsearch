/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransportGetIndexTemplatesAction extends TransportLocalProjectMetadataAction<
    GetIndexTemplatesRequest,
    GetIndexTemplatesResponse> {

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @SuppressWarnings("this-escape")
    @Inject
    public TransportGetIndexTemplatesAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            GetIndexTemplatesAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            projectResolver
        );

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            GetIndexTemplatesRequest::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetIndexTemplatesRequest request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetIndexTemplatesRequest request,
        ProjectState state,
        ActionListener<GetIndexTemplatesResponse> listener
    ) {
        final var cancellableTask = (CancellableTask) task;
        List<IndexTemplateMetadata> results;

        // If we did not ask for a specific name, then we return all templates
        if (request.names().length == 0) {
            results = new ArrayList<>(state.metadata().templates().values());
        } else {
            results = new ArrayList<>();
        }

        for (String name : request.names()) {
            if (Regex.isSimpleMatchPattern(name)) {
                for (Map.Entry<String, IndexTemplateMetadata> entry : state.metadata().templates().entrySet()) {
                    if (Regex.simpleMatch(name, entry.getKey())) {
                        results.add(entry.getValue());
                    }
                }
            } else if (state.metadata().templates().containsKey(name)) {
                results.add(state.metadata().templates().get(name));
            }
        }

        cancellableTask.ensureNotCancelled();
        listener.onResponse(new GetIndexTemplatesResponse(results));
    }
}
