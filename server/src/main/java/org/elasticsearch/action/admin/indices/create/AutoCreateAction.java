/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Api that auto creates an index or data stream that originate from requests that write into an index that doesn't yet exist.
 */
public final class AutoCreateAction extends ActionType<CreateIndexResponse> {

    private static final Logger logger = LogManager.getLogger(AutoCreateAction.class);

    public static final AutoCreateAction INSTANCE = new AutoCreateAction();
    public static final String NAME = "indices:admin/auto_create";

    private AutoCreateAction() {
        super(NAME, CreateIndexResponse::new);
    }

    public static final class TransportAction extends TransportMasterNodeAction<CreateIndexRequest, CreateIndexResponse> {

        private final ActiveShardsObserver activeShardsObserver;
        private final MetadataCreateIndexService createIndexService;
        private final MetadataCreateDataStreamService metadataCreateDataStreamService;
        private final AutoCreateIndex autoCreateIndex;
        private final SystemIndices systemIndices;

        @Inject
        public TransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               MetadataCreateIndexService createIndexService,
                               MetadataCreateDataStreamService metadataCreateDataStreamService,
                               AutoCreateIndex autoCreateIndex, SystemIndices systemIndices) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, CreateIndexRequest::new, indexNameExpressionResolver,
                    CreateIndexResponse::new, ThreadPool.Names.SAME);
            this.systemIndices = systemIndices;
            this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
            this.createIndexService = createIndexService;
            this.metadataCreateDataStreamService = metadataCreateDataStreamService;
            this.autoCreateIndex = autoCreateIndex;
        }

        @Override
        protected void masterOperation(Task task,
                                       CreateIndexRequest request,
                                       ClusterState state,
                                       ActionListener<CreateIndexResponse> finalListener) {
            AtomicReference<String> indexNameRef = new AtomicReference<>();
            ActionListener<AcknowledgedResponse> listener = ActionListener.wrap(
                response -> {
                    String indexName = indexNameRef.get();
                    assert indexName != null;
                    if (response.isAcknowledged()) {
                        activeShardsObserver.waitForActiveShards(
                            new String[]{indexName},
                            ActiveShardCount.DEFAULT,
                            request.timeout(),
                            shardsAcked -> {
                                finalListener.onResponse(new CreateIndexResponse(true, shardsAcked, indexName));
                            },
                            finalListener::onFailure
                        );
                    } else {
                        finalListener.onResponse(new CreateIndexResponse(false, false, indexName));
                    }
                },
                finalListener::onFailure
            );
            clusterService.submitStateUpdateTask("auto create [" + request.index() + "]",
                new AckedClusterStateUpdateTask(Priority.URGENT, request, listener) {

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    final SystemDataStreamDescriptor dataStreamDescriptor =
                        systemIndices.validateDataStreamAccess(request.index(), threadPool.getThreadContext());
                    final boolean isSystemDataStream = dataStreamDescriptor != null;
                    final boolean isSystemIndex = isSystemDataStream == false && systemIndices.isSystemIndex(request.index());
                    final ComposableIndexTemplate template = resolveTemplate(request, currentState.metadata());
                    final boolean isDataStream = isSystemIndex == false &&
                        (isSystemDataStream || (template != null && template.getDataStreamTemplate() != null));

                    if (isDataStream) {
                        // This expression only evaluates to true when the argument is non-null and false
                        if (isSystemDataStream == false && Boolean.FALSE.equals(template.getAllowAutoCreate())) {
                            throw new IndexNotFoundException(
                                "composable template " + template.indexPatterns() + " forbids index auto creation"
                            );
                        }

                        CreateDataStreamClusterStateUpdateRequest createRequest = new CreateDataStreamClusterStateUpdateRequest(
                            request.index(),
                            dataStreamDescriptor,
                            request.masterNodeTimeout(),
                            request.timeout()
                        );
                        ClusterState clusterState = metadataCreateDataStreamService.createDataStream(createRequest, currentState);
                        indexNameRef.set(clusterState.metadata().dataStreams().get(request.index()).getIndices().get(0).getName());
                        return clusterState;
                    } else {
                        String indexName = indexNameExpressionResolver.resolveDateMathExpression(request.index());
                        indexNameRef.set(indexName);
                        if (isSystemIndex) {
                            if (indexName.equals(request.index()) == false) {
                                throw new IllegalStateException("system indices do not support date math expressions");
                            }
                        } else {
                            // This will throw an exception if the index does not exist and creating it is prohibited
                            final boolean shouldAutoCreate = autoCreateIndex.shouldAutoCreate(indexName, currentState);

                            if (shouldAutoCreate == false) {
                                // The index already exists.
                                return currentState;
                            }
                        }

                        final SystemIndexDescriptor mainDescriptor =
                            isSystemIndex ? systemIndices.findMatchingDescriptor(indexName) : null;
                        final boolean isManagedSystemIndex = mainDescriptor != null && mainDescriptor.isAutomaticallyManaged();

                        final CreateIndexClusterStateUpdateRequest updateRequest;

                        if (isManagedSystemIndex) {
                            final SystemIndexDescriptor descriptor =
                                mainDescriptor.getDescriptorCompatibleWith(state.nodes().getSmallestNonClientNodeVersion());
                            if (descriptor == null) {
                                final String message = mainDescriptor.getMinimumNodeVersionMessage("auto-create index");
                                logger.warn(message);
                                throw new IllegalStateException(message);
                            }

                            updateRequest = buildSystemIndexUpdateRequest(descriptor);
                        } else {
                            updateRequest = buildUpdateRequest(indexName);
                        }

                        return createIndexService.applyCreateIndexRequest(currentState, updateRequest, false);
                    }
                }

                private CreateIndexClusterStateUpdateRequest buildUpdateRequest(String indexName) {
                    CreateIndexClusterStateUpdateRequest updateRequest =
                        new CreateIndexClusterStateUpdateRequest(request.cause(), indexName, request.index())
                            .ackTimeout(request.timeout())
                            .masterNodeTimeout(request.masterNodeTimeout());
                    logger.debug("Auto-creating index {}", indexName);
                    return updateRequest;
                }

                private CreateIndexClusterStateUpdateRequest buildSystemIndexUpdateRequest(SystemIndexDescriptor descriptor) {
                    String mappings = descriptor.getMappings();
                    Settings settings = descriptor.getSettings();
                    String aliasName = descriptor.getAliasName();
                    String concreteIndexName = descriptor.getPrimaryIndex();

                    CreateIndexClusterStateUpdateRequest updateRequest =
                        new CreateIndexClusterStateUpdateRequest(request.cause(), concreteIndexName, request.index())
                            .ackTimeout(request.timeout())
                            .masterNodeTimeout(request.masterNodeTimeout());

                    updateRequest.waitForActiveShards(ActiveShardCount.ALL);

                    if (mappings != null) {
                        updateRequest.mappings(mappings);
                    }
                    if (settings != null) {
                        updateRequest.settings(settings);
                    }
                    if (aliasName != null) {
                        updateRequest.aliases(Set.of(new Alias(aliasName)));
                    }

                    logger.debug("Auto-creating system index {}", concreteIndexName);

                    return updateRequest;
                }
            });
        }

        @Override
        protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
        }
    }

    static ComposableIndexTemplate resolveTemplate(CreateIndexRequest request, Metadata metadata) {
        String v2Template = MetadataIndexTemplateService.findV2Template(metadata, request.index(), false);
        return v2Template != null ? metadata.templatesV2().get(v2Template) : null;
    }
}
