/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.reservedstate.ReservedComposableIndexTemplateAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Optional;
import java.util.Set;

public class TransportPutComponentTemplateAction extends AcknowledgedTransportMasterNodeAction<PutComponentTemplateAction.Request> {

    private final MetadataIndexTemplateService indexTemplateService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportPutComponentTemplateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexTemplateService indexTemplateService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            PutComponentTemplateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutComponentTemplateAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexTemplateService = indexTemplateService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected ClusterBlockException checkBlock(PutComponentTemplateAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        Task task,
        final PutComponentTemplateAction.Request request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        final var project = projectResolver.getProjectMetadata(state);
        final ComponentTemplate componentTemplate = indexTemplateService.normalizeComponentTemplate(request.componentTemplate());
        final ComponentTemplate existingTemplate = project.componentTemplates().get(request.name());
        if (existingTemplate != null) {
            if (request.create()) {
                listener.onFailure(new IllegalArgumentException("component template [" + request.name() + "] already exists"));
                return;
            }
            // We have an early return here in case the component template already exists and is identical in content. We still need to do
            // this check in the cluster state update task in case the cluster state changed since this check.
            if (componentTemplate.contentEquals(existingTemplate)) {
                listener.onResponse(AcknowledgedResponse.TRUE);
                return;
            }
        }

        indexTemplateService.putComponentTemplate(
            request.cause(),
            request.create(),
            request.name(),
            request.masterNodeTimeout(),
            componentTemplate,
            project.id(),
            listener
        );
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedComposableIndexTemplateAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(PutComponentTemplateAction.Request request) {
        return Set.of(ReservedComposableIndexTemplateAction.reservedComponentName(request.name()));
    }

    @Override
    @FixForMultiProject // does this need to be a more general concept?
    protected void validateForReservedState(PutComponentTemplateAction.Request request, ClusterState state) {
        super.validateForReservedState(request, state);

        validateForReservedState(
            ProjectStateRegistry.get(state).reservedStateMetadata(projectResolver.getProjectId()).values(),
            reservedStateHandlerName().get(),
            modifiedKeys(request),
            request::toString
        );
    }
}
