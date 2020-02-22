/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.mapping.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Put mapping action.
 */
public class TransportPutMappingAction extends TransportMasterNodeAction<PutMappingRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutMappingAction.class);

    private final MetaDataMappingService metaDataMappingService;
    private final RequestValidators<PutMappingRequest> requestValidators;

    @Inject
    public TransportPutMappingAction(
            final TransportService transportService,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final MetaDataMappingService metaDataMappingService,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final RequestValidators<PutMappingRequest> requestValidators) {
        super(PutMappingAction.NAME, transportService, clusterService, threadPool, actionFilters, PutMappingRequest::new,
            indexNameExpressionResolver);
        this.metaDataMappingService = metaDataMappingService;
        this.requestValidators = Objects.requireNonNull(requestValidators);
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
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
    protected void masterOperation(Task task, final PutMappingRequest request, final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) {
        try {
            final Index[] concreteIndices = request.getConcreteIndex() == null ?
                indexNameExpressionResolver.concreteIndices(state, request)
                : new Index[] {request.getConcreteIndex()};
            final Optional<Exception> maybeValidationException = requestValidators.validateRequest(request, state, concreteIndices);
            if (maybeValidationException.isPresent()) {
                listener.onFailure(maybeValidationException.get());
                return;
            }
            PutMappingClusterStateUpdateRequest updateRequest = new PutMappingClusterStateUpdateRequest(request.source())
                .indices(concreteIndices)
                .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout());

            metaDataMappingService.putMapping(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {

                @Override
                public void onResponse(ClusterStateUpdateResponse response) {
                    listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
                }

                @Override
                public void onFailure(Exception t) {
                    logger.debug(() -> new ParameterizedMessage("failed to put mappings on indices [{}]",
                        Arrays.asList(concreteIndices)), t);
                    listener.onFailure(t);
                }
            });
        } catch (IndexNotFoundException ex) {
            logger.debug(() -> new ParameterizedMessage("failed to put mappings on indices [{}]",
                Arrays.asList(request.indices())), ex);
            throw ex;
        }
    }

}
