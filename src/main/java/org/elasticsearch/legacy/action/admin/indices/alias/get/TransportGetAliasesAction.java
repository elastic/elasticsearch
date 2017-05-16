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
package org.elasticsearch.legacy.action.admin.indices.alias.get;

import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.ActionListener;
import org.elasticsearch.legacy.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.metadata.AliasMetaData;
import org.elasticsearch.legacy.common.collect.ImmutableOpenMap;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

import java.util.List;

/**
 */
public class TransportGetAliasesAction extends TransportMasterNodeReadOperationAction<GetAliasesRequest, GetAliasesResponse> {

    @Inject
    public TransportGetAliasesAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
        super(settings, GetAliasesAction.NAME, transportService, clusterService, threadPool);
    }

    @Override
    protected String executor() {
        // very lightweight operation all in memory no need to fork to a thread pool
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetAliasesRequest newRequest() {
        return new GetAliasesRequest();
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
