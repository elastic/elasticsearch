/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.mapping.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataMappingService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Put mapping action.
 */
public class TransportPutMappingAction extends AcknowledgedTransportMasterNodeAction<PutMappingRequest> {

    private static final Logger logger = LogManager.getLogger(TransportPutMappingAction.class);

    private final MetadataMappingService metadataMappingService;
    private final RequestValidators<PutMappingRequest> requestValidators;
    private final SystemIndices systemIndices;

    @Inject
    public TransportPutMappingAction(
            final TransportService transportService,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final MetadataMappingService metadataMappingService,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final RequestValidators<PutMappingRequest> requestValidators,
            final SystemIndices systemIndices) {
        super(PutMappingAction.NAME, transportService, clusterService, threadPool, actionFilters, PutMappingRequest::new,
            indexNameExpressionResolver, ThreadPool.Names.SAME);
        this.metadataMappingService = metadataMappingService;
        this.requestValidators = Objects.requireNonNull(requestValidators);
        this.systemIndices = systemIndices;
    }

    @Override
    protected ClusterBlockException checkBlock(PutMappingRequest request, ClusterState state) {
        String[] indices;
        if (request.getConcreteIndex() == null) {
            indices = indexNameExpressionResolver.concreteIndexNames(state, request);
        } else {
            indices = new String[] {request.getConcreteIndex().getName()};
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indices);
    }

    @Override
    protected void masterOperation(final PutMappingRequest request, final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) {
        try {
            final Index[] concreteIndices = resolveIndices(state, request, indexNameExpressionResolver);
            final String mappingSource = request.source();

            final Optional<Exception> maybeValidationException = requestValidators.validateRequest(request, state, concreteIndices);
            if (maybeValidationException.isPresent()) {
                listener.onFailure(maybeValidationException.get());
                return;
            }

            final List<String> violations = checkForSystemIndexViolations(concreteIndices, mappingSource);
            if (violations.isEmpty() == false) {
                final String message = "Cannot update mappings in "
                    + violations
                    + ": system indices can only use mappings from their descriptors, "
                    + "but the mappings in the request did not match those in the descriptors(s)";
                logger.warn(message);
                listener.onFailure(new IllegalArgumentException(message));
                return;
            }

            performMappingUpdate(concreteIndices, request, listener, metadataMappingService);
        } catch (IndexNotFoundException ex) {
            logger.debug(() -> new ParameterizedMessage("failed to put mappings on indices [{}], type [{}]",
                request.indices(), request.type()), ex);
            throw ex;
        }
    }

    static Index[] resolveIndices(final ClusterState state, PutMappingRequest request, final IndexNameExpressionResolver iner) {
        if (request.getConcreteIndex() == null) {
            if (request.writeIndexOnly()) {
                List<Index> indices = new ArrayList<>();
                for (String indexExpression : request.indices()) {
                    indices.add(iner.concreteWriteIndex(state, request.indicesOptions(), indexExpression,
                        request.indicesOptions().allowNoIndices(), request.includeDataStreams()));
                }
                return indices.toArray(Index.EMPTY_ARRAY);
            } else {
                return iner.concreteIndices(state, request);
            }
        } else {
            return new Index[]{request.getConcreteIndex()};
        }
    }

    static void performMappingUpdate(Index[] concreteIndices,
                                     PutMappingRequest request,
                                     ActionListener<AcknowledgedResponse> listener,
                                     MetadataMappingService metadataMappingService) {
        PutMappingClusterStateUpdateRequest updateRequest = new PutMappingClusterStateUpdateRequest()
                    .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
                    .indices(concreteIndices).type(request.type())
                    .source(request.source());

        metadataMappingService.putMapping(updateRequest, new ActionListener<AcknowledgedResponse>() {

            @Override
            public void onResponse(AcknowledgedResponse response) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception t) {
                logger.debug(() -> new ParameterizedMessage("failed to put mappings on indices [{}]",
                    Arrays.asList(concreteIndices)), t);
                listener.onFailure(t);
            }
        });
    }

    private List<String> checkForSystemIndexViolations(Index[] concreteIndices, String requestMappings) {
        List<String> violations = new ArrayList<>();

        for (Index index : concreteIndices) {
            final SystemIndexDescriptor descriptor = systemIndices.findMatchingDescriptor(index.getName());
            if (descriptor != null && descriptor.isAutomaticallyManaged()) {
                final String descriptorMappings = descriptor.getMappings();

                // Technically we could trip over a difference in whitespace here, but then again nobody should be trying to manually
                // update a descriptor's mappings.
                if (descriptorMappings.equals(requestMappings) == false) {
                    violations.add(index.getName());
                }
            }
        }
        return violations;
    }
}
