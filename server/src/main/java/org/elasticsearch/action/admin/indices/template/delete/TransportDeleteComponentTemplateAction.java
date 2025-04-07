/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.template.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.template.reservedstate.ReservedComposableIndexTemplateAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class TransportDeleteComponentTemplateAction extends AcknowledgedTransportMasterNodeAction<
    TransportDeleteComponentTemplateAction.Request> {

    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>("cluster:admin/component_template/delete");

    private final MetadataIndexTemplateService indexTemplateService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportDeleteComponentTemplateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexTemplateService indexTemplateService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(TYPE.name(), transportService, clusterService, threadPool, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.indexTemplateService = indexTemplateService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        Task task,
        final Request request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        var project = projectResolver.getProjectMetadata(state);
        indexTemplateService.removeComponentTemplate(request.names(), request.masterNodeTimeout(), project, listener);
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedComposableIndexTemplateAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(Request request) {
        return Arrays.stream(request.names())
            .map(n -> ReservedComposableIndexTemplateAction.reservedComponentName(n))
            .collect(Collectors.toSet());
    }

    public static class Request extends MasterNodeRequest<Request> {

        private final String[] names;

        public Request(StreamInput in) throws IOException {
            super(in);
            names = in.readStringArray();
        }

        /**
         * Constructs a new delete index request for the specified name.
         */
        public Request(String... names) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
            this.names = Objects.requireNonNull(names, "component templates to delete must not be null");
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Arrays.stream(names).anyMatch(Strings::hasLength) == false) {
                validationException = addValidationError("no component template names specified", validationException);
            }
            return validationException;
        }

        /**
         * The index template names to delete.
         */
        public String[] names() {
            return names;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
        }
    }
}
