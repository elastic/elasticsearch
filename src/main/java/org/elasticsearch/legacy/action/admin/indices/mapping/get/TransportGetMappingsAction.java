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

package org.elasticsearch.legacy.action.admin.indices.mapping.get;

import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.ActionListener;
import org.elasticsearch.legacy.action.support.master.info.TransportClusterInfoAction;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.metadata.MappingMetaData;
import org.elasticsearch.legacy.common.collect.ImmutableOpenMap;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

/**
 */
public class TransportGetMappingsAction extends TransportClusterInfoAction<GetMappingsRequest, GetMappingsResponse> {

    @Inject
    public TransportGetMappingsAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
        super(settings, GetMappingsAction.NAME, transportService, clusterService, threadPool);
    }

    @Override
    protected GetMappingsRequest newRequest() {
        return new GetMappingsRequest();
    }

    @Override
    protected GetMappingsResponse newResponse() {
        return new GetMappingsResponse();
    }

    @Override
    protected void doMasterOperation(final GetMappingsRequest request, String[] concreteIndices, final ClusterState state, final ActionListener<GetMappingsResponse> listener) throws ElasticsearchException {
        logger.trace("serving getMapping request based on version {}", state.version());
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> result = state.metaData().findMappings(
                concreteIndices, request.types()
        );
        listener.onResponse(new GetMappingsResponse(result));
    }
}
