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
package org.elasticsearch.action.admin.indices.exists.types;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Types exists transport action.
 */
public class TransportTypesExistsAction extends TransportMasterNodeReadOperationAction<TypesExistsRequest, TypesExistsResponse> {

    @Inject
    public TransportTypesExistsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, TypesExistsAction.NAME, transportService, clusterService, threadPool, actionFilters, TypesExistsRequest.class);
    }

    @Override
    protected String executor() {
        // lightweight check
        return ThreadPool.Names.SAME;
    }

    @Override
    protected TypesExistsResponse newResponse() {
        return new TypesExistsResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(TypesExistsRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, state.metaData().concreteIndices(request.indicesOptions(), request.indices()));
    }

    @Override
    protected void masterOperation(final TypesExistsRequest request, final ClusterState state, final ActionListener<TypesExistsResponse> listener) throws ElasticsearchException {
        String[] concreteIndices = state.metaData().concreteIndices(request.indicesOptions(), request.indices());
        if (concreteIndices.length == 0) {
            listener.onResponse(new TypesExistsResponse(false));
            return;
        }

        for (String concreteIndex : concreteIndices) {
            if (!state.metaData().hasConcreteIndex(concreteIndex)) {
                listener.onResponse(new TypesExistsResponse(false));
                return;
            }

            ImmutableOpenMap<String, MappingMetaData> mappings = state.metaData().getIndices().get(concreteIndex).mappings();
            if (mappings.isEmpty()) {
                listener.onResponse(new TypesExistsResponse(false));
                return;
            }

            for (String type : request.types()) {
                if (!mappings.containsKey(type)) {
                    listener.onResponse(new TypesExistsResponse(false));
                    return;
                }
            }
        }

        listener.onResponse(new TypesExistsResponse(true));
    }
}
