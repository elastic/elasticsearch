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
package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 */
public class TransportGetAliasesAction extends TransportMasterNodeReadOperationAction<GetAliasesRequest, GetAliasesResponse> {

    @Inject
    public TransportGetAliasesAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, GetAliasesAction.NAME, transportService, clusterService, threadPool, actionFilters, GetAliasesRequest.class);
    }

    @Override
    protected String executor() {
        // very lightweight operation all in memory no need to fork to a thread pool
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(GetAliasesRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, state.metaData().concreteIndices(request.indicesOptions(), request.indices()));
    }

    @Override
    protected GetAliasesResponse newResponse() {
        return new GetAliasesResponse();
    }

    @Override
    protected void masterOperation(GetAliasesRequest request, ClusterState state, ActionListener<GetAliasesResponse> listener) throws ElasticsearchException {
        String[] concreteIndices = state.metaData().concreteIndices(request.indicesOptions(), request.indices());
        @SuppressWarnings("unchecked") // ImmutableList to List results incompatible type
                ImmutableOpenMap<String, List<AliasMetaData>> result = (ImmutableOpenMap) state.metaData().findAliases(request.aliases(), concreteIndices);
        listener.onResponse(new GetAliasesResponse(result));
    }

}
