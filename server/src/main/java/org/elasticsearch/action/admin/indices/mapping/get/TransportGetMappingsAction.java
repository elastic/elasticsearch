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

package org.elasticsearch.action.admin.indices.mapping.get;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.info.TransportClusterInfoAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportGetMappingsAction extends TransportClusterInfoAction<GetMappingsRequest, GetMappingsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetMappingsAction.class);

    private final IndicesService indicesService;

    @Inject
    public TransportGetMappingsAction(TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver, IndicesService indicesService) {
        super(GetMappingsAction.NAME, transportService, clusterService, threadPool, actionFilters, GetMappingsRequest::new,
                indexNameExpressionResolver);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        // very lightweight operation, no need to fork
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(GetMappingsRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected GetMappingsResponse read(StreamInput in) throws IOException {
        return new GetMappingsResponse(in);
    }

    @Override
    protected void doMasterOperation(final GetMappingsRequest request, String[] concreteIndices, final ClusterState state,
                                     final ActionListener<GetMappingsResponse> listener) {
        logger.trace("serving getMapping request based on version {}", state.version());
        try {
            ImmutableOpenMap<String, MappingMetadata> result =
                    state.metadata().findMappings(concreteIndices, indicesService.getFieldFilter());
            listener.onResponse(new GetMappingsResponse(result));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }
}
