/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.create;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionMultiListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_HIDDEN;
import static org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener.rerouteCompletionIsNotRequired;

/**
 * Api that auto creates an index or data stream that originate from requests that write into an index that doesn't yet exist.
 */
public final class AutoCreateAction extends ActionType<CreateIndexResponse> {

    private static final Logger logger = LogManager.getLogger(AutoCreateAction.class);

    public static final AutoCreateAction INSTANCE = new AutoCreateAction();
    public static final String NAME = "indices:admin/auto_create";

    private AutoCreateAction() {
        super(NAME);
    }

    public static final class TransportAction extends TransportMasterNodeAction<CreateIndexRequest, CreateIndexResponse> {

        private final MetadataCreateIndexService createIndexService;
        private final MetadataCreateDataStreamService metadataCreateDataStreamService;
        private final AutoCreateIndex autoCreateIndex;
        private final ProjectResolver projectResolver;
        private final SystemIndices systemIndices;

        private final MasterServiceTaskQueue<CreateIndexTask> taskQueue;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            MetadataCreateIndexService createIndexService,
            MetadataCreateDataStreamService metadataCreateDataStreamService,
            AutoCreateIndex autoCreateIndex,
            SystemIndices systemIndices,
            AllocationService allocationService,
            ProjectResolver projectResolver
        ) {
            super(
                NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                CreateIndexRequest::new,
                CreateIndexResponse::new,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            this.systemIndices = systemIndices;
            this.createIndexService = createIndexService;
            this.metadataCreateDataStreamService = metadataCreateDataStreamService;
            this.autoCreateIndex = autoCreateIndex;
            this.projectResolver = projectResolver;
            this.taskQueue = clusterService.createTaskQueue("auto-create", Priority.URGENT, batchExecutionContext -> {
                final var listener = new AllocationActionMultiListener<CreateIndexResponse>(threadPool.getThreadContext());
                final var taskContexts = batchExecutionContext.taskContexts();
                final var successfulRequests = Maps.<CreateIndexRequest, List<String>>newMapWithExpectedSize(taskContexts.size());
                var state = batchExecutionContext.initialState();
                for (final var taskContext : taskContexts) {
                    final var task = taskContext.getTask();
                    try (var ignored = taskContext.captureResponseHeaders()) {
                        state = task.execute(state, successfulRequests, taskContext, listener);
                        assert successfulRequests.containsKey(task.request);
                    } catch (Exception e) {
                        taskContext.onFailure(e);
                    }
                }
                if (state != batchExecutionContext.initialState()) {
                    try (var ignored = batchExecutionContext.dropHeadersContext()) {
                        state = allocationService.reroute(state, "auto-create", listener.reroute());
                    }
                } else {
                    listener.noRerouteNeeded();
                }
                return state;
            });
        }

        @Override
        protected void masterOperation(
            Task task,
            CreateIndexRequest request,
            ClusterState state,
            ActionListener<CreateIndexResponse> listener
        ) {
            taskQueue.submitTask(
                "auto create [" + request.index() + "]",
                new CreateIndexTask(request, projectResolver.getProjectId(), listener),
                request.masterNodeTimeout()
            );
        }

        @Override
        protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
            return state.blocks().indexBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE, request.index());
        }

        private final class CreateIndexTask implements ClusterStateTaskListener {
            private final CreateIndexRequest request;
            private final ProjectId projectId;
            private final ActionListener<CreateIndexResponse> listener;

            private CreateIndexTask(CreateIndexRequest request, ProjectId projectId, ActionListener<CreateIndexResponse> listener) {
                this.request = request;
                this.projectId = projectId;
                this.listener = listener;
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            private ClusterStateAckListener getAckListener(
                String indexName,
                AllocationActionMultiListener<CreateIndexResponse> allocationActionMultiListener
            ) {
                return getAckListener(List.of(indexName), allocationActionMultiListener);
            }

            private ClusterStateAckListener getAckListener(
                List<String> indexNames,
                AllocationActionMultiListener<CreateIndexResponse> allocationActionMultiListener
            ) {
                return new ClusterStateAckListener() {
                    @Override
                    public boolean mustAck(DiscoveryNode discoveryNode) {
                        return true;
                    }

                    @Override
                    public void onAllNodesAcked() {
                        ActiveShardsObserver.waitForActiveShards(
                            clusterService,
                            projectId,
                            indexNames.toArray(String[]::new),
                            ActiveShardCount.DEFAULT,
                            request.ackTimeout(),
                            allocationActionMultiListener.delay(listener)
                                .map(shardsAcked -> new CreateIndexResponse(true, shardsAcked, indexNames.get(0)))
                        );
                    }

                    @Override
                    public void onAckFailure(Exception e) {
                        allocationActionMultiListener.delay(listener).onResponse(new CreateIndexResponse(false, false, indexNames.get(0)));
                    }

                    @Override
                    public void onAckTimeout() {
                        allocationActionMultiListener.delay(listener).onResponse(new CreateIndexResponse(false, false, indexNames.get(0)));
                    }

                    @Override
                    public TimeValue ackTimeout() {
                        return request.ackTimeout();
                    }
                };
            }

            /**
             * @param successfulRequests Cache of successful requests executed by this batch, to avoid failing duplicate requests with a
             *                           {@link ResourceAlreadyExistsException}. If this method executes a request it should update this
             *                           map.
             */
            ClusterState execute(
                ClusterState currentState,
                Map<CreateIndexRequest, List<String>> successfulRequests,
                ClusterStateTaskExecutor.TaskContext<CreateIndexTask> taskContext,
                AllocationActionMultiListener<CreateIndexResponse> allocationActionMultiListener
            ) throws Exception {
                final var previousIndexName = successfulRequests.get(request);
                if (previousIndexName != null) {
                    taskContext.success(getAckListener(previousIndexName, allocationActionMultiListener));
                    return currentState;
                }

                final SystemDataStreamDescriptor dataStreamDescriptor = systemIndices.validateDataStreamAccess(
                    request.index(),
                    threadPool.getThreadContext()
                );
                final boolean isSystemDataStream = dataStreamDescriptor != null;
                final boolean isSystemIndex = isSystemDataStream == false && systemIndices.isSystemIndex(request.index());
                final ComposableIndexTemplate template = resolveTemplate(request, currentState.metadata().getProject(projectId));
                final boolean isDataStream = isSystemIndex == false
                    && (isSystemDataStream || (template != null && template.getDataStreamTemplate() != null));

                if (isDataStream) {
                    // This expression only evaluates to true when the argument is non-null and false
                    if (isSystemDataStream == false && Boolean.FALSE.equals(template.getAllowAutoCreate())) {
                        throw new IndexNotFoundException(
                            "composable template " + template.indexPatterns() + " forbids index auto creation",
                            request.index()
                        );
                    }

                    CreateDataStreamClusterStateUpdateRequest createRequest = new CreateDataStreamClusterStateUpdateRequest(
                        projectId,
                        request.index(),
                        dataStreamDescriptor,
                        request.masterNodeTimeout(),
                        request.ackTimeout(),
                        false
                    );
                    assert createRequest.performReroute() == false
                        : "rerouteCompletionIsNotRequired() assumes reroute is not called by underlying service";
                    ClusterState clusterState = metadataCreateDataStreamService.createDataStream(
                        createRequest,
                        currentState,
                        rerouteCompletionIsNotRequired(),
                        request.isInitializeFailureStore()
                    );

                    final var dataStream = clusterState.metadata().getProject(projectId).dataStreams().get(request.index());
                    final var backingIndexName = dataStream.getIndices().get(0).getName();
                    final var indexNames = dataStream.getFailureIndices().isEmpty()
                        ? List.of(backingIndexName)
                        : List.of(backingIndexName, dataStream.getFailureIndices().get(0).getName());
                    taskContext.success(getAckListener(indexNames, allocationActionMultiListener));
                    successfulRequests.put(request, indexNames);
                    return clusterState;
                } else {
                    if (request.isRequireDataStream()) {
                        throw new IndexNotFoundException(
                            "the index creation request requires a data stream, "
                                + "but no matching index template with data stream template was found for it",
                            request.index()
                        );
                    }
                    final var indexName = IndexNameExpressionResolver.resolveDateMathExpression(request.index());
                    if (isSystemIndex) {
                        if (indexName.equals(request.index()) == false) {
                            throw new IllegalStateException("system indices do not support date math expressions");
                        }
                    } else {
                        // This will throw an exception if the index does not exist and creating it is prohibited
                        final boolean shouldAutoCreate = autoCreateIndex.shouldAutoCreate(
                            indexName,
                            currentState.metadata().getProject(projectId)
                        );

                        if (shouldAutoCreate == false) {
                            // The index already exists.
                            taskContext.success(getAckListener(indexName, allocationActionMultiListener));
                            successfulRequests.put(request, List.of(indexName));
                            return currentState;
                        }
                    }

                    final SystemIndexDescriptor mainDescriptor = isSystemIndex ? systemIndices.findMatchingDescriptor(indexName) : null;
                    final boolean isManagedSystemIndex = mainDescriptor != null && mainDescriptor.isAutomaticallyManaged();

                    final CreateIndexClusterStateUpdateRequest updateRequest;

                    if (isManagedSystemIndex) {
                        final var requiredMinimumMappingVersion = currentState.getMinSystemIndexMappingVersions()
                            .get(mainDescriptor.getPrimaryIndex());
                        final SystemIndexDescriptor descriptor = mainDescriptor.getDescriptorCompatibleWith(requiredMinimumMappingVersion);
                        if (descriptor == null) {
                            final String message = mainDescriptor.getMinimumMappingsVersionMessage(
                                "auto-create index",
                                requiredMinimumMappingVersion
                            );
                            logger.warn(message);
                            throw new IllegalStateException(message);
                        }

                        updateRequest = buildSystemIndexUpdateRequest(projectId, indexName, descriptor);
                    } else if (isSystemIndex) {
                        updateRequest = buildUpdateRequest(projectId, indexName);

                        if (Objects.isNull(request.settings())) {
                            updateRequest.settings(SystemIndexDescriptor.DEFAULT_SETTINGS);
                        } else if (false == request.settings().hasValue(SETTING_INDEX_HIDDEN)) {
                            updateRequest.settings(Settings.builder().put(request.settings()).put(SETTING_INDEX_HIDDEN, true).build());
                        } else if ("false".equals(request.settings().get(SETTING_INDEX_HIDDEN))) {
                            final String message = "Cannot auto-create system index [" + indexName + "] with [index.hidden] set to 'false'";
                            logger.warn(message);
                            throw new IllegalStateException(message);
                        }
                    } else {
                        updateRequest = buildUpdateRequest(projectId, indexName);
                    }

                    assert updateRequest.performReroute() == false
                        : "rerouteCompletionIsNotRequired() assumes reroute is not called by underlying service";
                    final var clusterState = createIndexService.applyCreateIndexRequest(
                        currentState,
                        updateRequest,
                        false,
                        rerouteCompletionIsNotRequired()
                    );
                    taskContext.success(getAckListener(indexName, allocationActionMultiListener));
                    successfulRequests.put(request, List.of(indexName));
                    return clusterState;
                }
            }

            private CreateIndexClusterStateUpdateRequest buildUpdateRequest(ProjectId projectId, String indexName) {
                CreateIndexClusterStateUpdateRequest updateRequest = new CreateIndexClusterStateUpdateRequest(
                    request.cause(),
                    projectId,
                    indexName,
                    request.index()
                ).performReroute(false);
                logger.debug("Auto-creating index {}", indexName);
                return updateRequest;
            }

            private CreateIndexClusterStateUpdateRequest buildSystemIndexUpdateRequest(
                ProjectId projectId,
                String indexName,
                SystemIndexDescriptor descriptor
            ) {
                String mappings = descriptor.getMappings();
                Settings settings = descriptor.getSettings();
                String aliasName = descriptor.getAliasName();

                // if we are writing to the alias name, we should create the primary index here
                String concreteIndexName = indexName.equals(aliasName) ? descriptor.getPrimaryIndex() : indexName;

                CreateIndexClusterStateUpdateRequest updateRequest = new CreateIndexClusterStateUpdateRequest(
                    request.cause(),
                    projectId,
                    concreteIndexName,
                    request.index()
                ).performReroute(false);

                updateRequest.waitForActiveShards(ActiveShardCount.ALL);

                if (mappings != null) {
                    updateRequest.mappings(mappings);
                }
                if (settings != null) {
                    updateRequest.settings(settings);
                }
                if (aliasName != null) {
                    Alias systemAlias = new Alias(aliasName).isHidden(true);
                    if (concreteIndexName.equals(descriptor.getPrimaryIndex())) {
                        systemAlias.writeIndex(true);
                    }
                    updateRequest.aliases(Set.of(systemAlias));
                }

                if (logger.isDebugEnabled()) {
                    if (concreteIndexName.equals(indexName) == false) {
                        logger.debug("Auto-creating backing system index {} for alias {}", concreteIndexName, indexName);
                    } else {
                        logger.debug("Auto-creating system index {}", concreteIndexName);
                    }
                }

                return updateRequest;
            }
        }
    }

    static ComposableIndexTemplate resolveTemplate(CreateIndexRequest request, ProjectMetadata projectMetadata) {
        String v2Template = MetadataIndexTemplateService.findV2Template(projectMetadata, request.index(), false);
        return v2Template != null ? projectMetadata.templatesV2().get(v2Template) : null;
    }
}
