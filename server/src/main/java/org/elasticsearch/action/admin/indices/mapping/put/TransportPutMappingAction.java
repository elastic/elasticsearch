/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.mapping.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataMappingService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;

/**
 * Put mapping action.
 */
public class TransportPutMappingAction extends AcknowledgedTransportMasterNodeAction<PutMappingRequest> {

    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>("indices:admin/mapping/put");
    private static final Logger logger = LogManager.getLogger(TransportPutMappingAction.class);

    private final MetadataMappingService metadataMappingService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final RequestValidators<PutMappingRequest> requestValidators;
    private final SystemIndices systemIndices;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportPutMappingAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final MetadataMappingService metadataMappingService,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final RequestValidators<PutMappingRequest> requestValidators,
        final SystemIndices systemIndices,
        final ProjectResolver projectResolver
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutMappingRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.metadataMappingService = metadataMappingService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.requestValidators = Objects.requireNonNull(requestValidators);
        this.systemIndices = systemIndices;
        this.projectResolver = projectResolver;
    }

    @Override
    protected ClusterBlockException checkBlock(PutMappingRequest request, ClusterState state) {
        final ProjectId projectId = projectResolver.getProjectId();
        String[] indices;
        if (request.getConcreteIndex() == null) {
            ProjectMetadata projectMetadata = state.metadata().getProject(projectId);
            indices = indexNameExpressionResolver.concreteIndexNames(projectMetadata, request);
        } else {
            indices = new String[] { request.getConcreteIndex().getName() };
        }
        return state.blocks().indicesBlockedException(projectId, ClusterBlockLevel.METADATA_WRITE, indices);
    }

    @Override
    protected void masterOperation(
        Task task,
        final PutMappingRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        try {
            ProjectMetadata projectMetadata = projectResolver.getProjectMetadata(state);
            final Index[] concreteIndices = resolveIndices(projectMetadata, request, indexNameExpressionResolver);

            final Optional<Exception> maybeValidationException = requestValidators.validateRequest(
                request,
                projectMetadata,
                concreteIndices
            );
            if (maybeValidationException.isPresent()) {
                listener.onFailure(maybeValidationException.get());
                return;
            }

            String message = checkForFailureStoreViolations(projectMetadata, concreteIndices, request);
            if (message != null) {
                logger.warn(message);
                listener.onFailure(new IllegalStateException(message));
                return;
            }

            message = checkForSystemIndexViolations(systemIndices, concreteIndices, request);
            if (message != null) {
                logger.warn(message);
                listener.onFailure(new IllegalStateException(message));
                return;
            }

            performMappingUpdate(concreteIndices, request, listener, metadataMappingService, false);
        } catch (IndexNotFoundException ex) {
            logger.debug(() -> "failed to put mappings on indices " + Arrays.toString(request.indices()), ex);
            throw ex;
        }
    }

    static Index[] resolveIndices(
        final ProjectMetadata projectMetadata,
        PutMappingRequest request,
        final IndexNameExpressionResolver iner
    ) {
        if (request.getConcreteIndex() == null) {
            if (request.writeIndexOnly()) {
                List<Index> indices = new ArrayList<>();
                for (String indexExpression : request.indices()) {
                    indices.add(
                        iner.concreteWriteIndex(
                            projectMetadata,
                            request.indicesOptions(),
                            indexExpression,
                            request.indicesOptions().allowNoIndices(),
                            request.includeDataStreams()
                        )
                    );
                }
                return indices.toArray(Index.EMPTY_ARRAY);
            } else {
                return iner.concreteIndices(projectMetadata, request);
            }
        } else {
            return new Index[] { request.getConcreteIndex() };
        }
    }

    static void performMappingUpdate(
        Index[] concreteIndices,
        PutMappingRequest request,
        ActionListener<AcknowledgedResponse> listener,
        MetadataMappingService metadataMappingService,
        boolean autoUpdate
    ) {
        ActionListener.run(listener.delegateResponse((l, e) -> {
            logger.debug(() -> "failed to put mappings on indices " + Arrays.toString(concreteIndices), e);
            l.onFailure(e);
        }),
            wrappedListener -> metadataMappingService.putMapping(
                new PutMappingClusterStateUpdateRequest(
                    request.masterNodeTimeout(),
                    request.ackTimeout(),
                    request.source(),
                    autoUpdate,
                    concreteIndices
                ),
                wrappedListener
            )
        );
    }

    static String checkForFailureStoreViolations(ProjectMetadata projectMetadata, Index[] concreteIndices, PutMappingRequest request) {
        // Requests that a cluster generates itself are permitted to make changes to mappings
        // so that rolling upgrade scenarios still work. We check this via the request's origin.
        if (Strings.isNullOrEmpty(request.origin()) == false) {
            return null;
        }

        List<String> violations = new ArrayList<>();
        SortedMap<String, IndexAbstraction> indicesLookup = projectMetadata.getIndicesLookup();
        for (Index index : concreteIndices) {
            IndexAbstraction indexAbstraction = indicesLookup.get(index.getName());
            if (indexAbstraction != null) {
                DataStream maybeDataStream = indexAbstraction.getParentDataStream();
                if (maybeDataStream != null && maybeDataStream.isFailureStoreIndex(index.getName())) {
                    violations.add(index.getName());
                }
            }
        }

        if (violations.isEmpty() == false) {
            return "Cannot update mappings in "
                + violations
                + ": mappings for indices contained in data stream failure stores cannot be updated";
        }
        return null;
    }

    static String checkForSystemIndexViolations(SystemIndices systemIndices, Index[] concreteIndices, PutMappingRequest request) {
        // Requests that a cluster generates itself are permitted to have a difference in mappings
        // so that rolling upgrade scenarios still work. We check this via the request's origin.
        if (Strings.isNullOrEmpty(request.origin()) == false) {
            return null;
        }

        List<String> violations = new ArrayList<>();

        final String requestMappings = request.source();

        for (Index index : concreteIndices) {
            final SystemIndexDescriptor descriptor = systemIndices.findMatchingDescriptor(index.getName());
            if (descriptor != null && descriptor.isAutomaticallyManaged() && descriptor.hasDynamicMappings() == false) {
                final String descriptorMappings = descriptor.getMappings();
                // Technically we could trip over a difference in whitespace here, but then again nobody should be trying to manually
                // update a descriptor's mappings.
                if (descriptorMappings.equals(requestMappings) == false) {
                    violations.add(index.getName());
                }
            }
        }

        if (violations.isEmpty() == false) {
            return "Cannot update mappings in "
                + violations
                + ": system indices can only use mappings from their descriptors, "
                + "but the mappings in the request ["
                + requestMappings
                + "] did not match those in the descriptor(s)";
        }

        return null;
    }
}
