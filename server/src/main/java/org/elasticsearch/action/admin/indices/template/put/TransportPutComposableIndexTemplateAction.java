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
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.template.reservedstate.ReservedComposableIndexTemplateAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.core.Strings.format;

public class TransportPutComposableIndexTemplateAction extends AcknowledgedTransportMasterNodeAction<
    TransportPutComposableIndexTemplateAction.Request> {

    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>("indices:admin/index_template/put");
    private final MetadataIndexTemplateService indexTemplateService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportPutComposableIndexTemplateAction(
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
        ProjectId projectId = projectResolver.getProjectId();
        verifyIfUsingReservedComponentTemplates(request, state.metadata().reservedStateMetadata().values());
        verifyIfUsingReservedComponentTemplates(request, state.metadata().getProject(projectId).reservedStateMetadata().values());
        ComposableIndexTemplate indexTemplate = request.indexTemplate();
        indexTemplateService.putIndexTemplateV2(
            request.cause(),
            request.create(),
            request.name(),
            request.masterNodeTimeout(),
            indexTemplate,
            projectId,
            listener
        );
    }

    public static void verifyIfUsingReservedComponentTemplates(Request request, Collection<ReservedStateMetadata> reservedStateMetadata) {
        ComposableIndexTemplate indexTemplate = request.indexTemplate();
        Set<String> composedOfKeys = indexTemplate.composedOf()
            .stream()
            .map(ReservedComposableIndexTemplateAction::reservedComponentName)
            .collect(Collectors.toSet());

        List<String> errors = new ArrayList<>();

        for (ReservedStateMetadata metadata : reservedStateMetadata) {
            Set<String> conflicts = metadata.conflicts(ReservedComposableIndexTemplateAction.NAME, composedOfKeys);
            if (conflicts.isEmpty() == false) {
                errors.add(format("[%s] is reserved by [%s]", String.join(", ", conflicts), metadata.namespace()));
            }
        }

        if (errors.isEmpty() == false) {
            throw new IllegalArgumentException(
                format("Failed to process request [%s] with errors: [%s]", request, String.join(", ", errors))
            );
        }
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedComposableIndexTemplateAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(Request request) {
        return Set.of(ReservedComposableIndexTemplateAction.reservedComposableIndexName(request.name()));
    }

    @Override
    protected void validateForReservedState(Request request, ClusterState state) {
        super.validateForReservedState(request, state);

        validateForReservedState(
            projectResolver.getProjectMetadata(state).reservedStateMetadata().values(),
            reservedStateHandlerName().get(),
            modifiedKeys(request),
            request.toString()
        );
    }

    /**
     * A request for putting a single index template into the cluster state
     */
    public static class Request extends MasterNodeRequest<Request> implements IndicesRequest {
        private final String name;
        @Nullable
        private String cause;
        private boolean create;
        private ComposableIndexTemplate indexTemplate;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.cause = in.readOptionalString();
            this.create = in.readBoolean();
            this.indexTemplate = new ComposableIndexTemplate(in);
        }

        /**
         * Constructs a new put index template request with the provided name.
         */
        public Request(String name) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
            this.name = name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeOptionalString(cause);
            out.writeBoolean(create);
            this.indexTemplate.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (name == null || Strings.hasText(name) == false) {
                validationException = addValidationError("name is missing", validationException);
            }
            validationException = validateIndexTemplate(validationException);
            return validationException;
        }

        public ActionRequestValidationException validateIndexTemplate(@Nullable ActionRequestValidationException validationException) {
            if (indexTemplate == null) {
                validationException = addValidationError("an index template is required", validationException);
            } else {
                if (indexTemplate.template() != null && indexTemplate.indexPatterns().stream().anyMatch(Regex::isMatchAllPattern)) {
                    if (indexTemplate.template().settings() != null
                        && IndexMetadata.INDEX_HIDDEN_SETTING.exists(indexTemplate.template().settings())) {
                        validationException = addValidationError(
                            "global composable templates may not specify the setting " + IndexMetadata.INDEX_HIDDEN_SETTING.getKey(),
                            validationException
                        );
                    }
                }
                if (indexTemplate.priority() != null && indexTemplate.priority() < 0) {
                    validationException = addValidationError("index template priority must be >= 0", validationException);
                }
            }
            return validationException;
        }

        /**
         * The name of the index template.
         */
        public String name() {
            return this.name;
        }

        /**
         * Set to {@code true} to force only creation, not an update of an index template. If it already
         * exists, it will fail with an {@link IllegalArgumentException}.
         */
        public Request create(boolean create) {
            this.create = create;
            return this;
        }

        public boolean create() {
            return create;
        }

        /**
         * The cause for this index template creation.
         */
        public Request cause(@Nullable String cause) {
            this.cause = cause;
            return this;
        }

        @Nullable
        public String cause() {
            return this.cause;
        }

        /**
         * The index template that will be inserted into the cluster state
         */
        public Request indexTemplate(ComposableIndexTemplate template) {
            this.indexTemplate = template;
            return this;
        }

        public ComposableIndexTemplate indexTemplate() {
            return this.indexTemplate;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("PutTemplateV2Request[");
            sb.append("name=").append(name);
            sb.append(", cause=").append(cause);
            sb.append(", create=").append(create);
            sb.append(", index_template=").append(indexTemplate);
            sb.append("]");
            return sb.toString();
        }

        @Override
        public String[] indices() {
            return indexTemplate.indexPatterns().toArray(Strings.EMPTY_ARRAY);
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictExpand();
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, cause, create, indexTemplate);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(this.name, other.name)
                && Objects.equals(this.cause, other.cause)
                && Objects.equals(this.indexTemplate, other.indexTemplate)
                && this.create == other.create;
        }
    }
}
