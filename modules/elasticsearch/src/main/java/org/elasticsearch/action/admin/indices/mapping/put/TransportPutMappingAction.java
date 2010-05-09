/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaDataService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.settings.Settings;

/**
 * Put mapping action.
 *
 * @author kimchy (shay.banon)
 */
public class TransportPutMappingAction extends TransportMasterNodeOperationAction<PutMappingRequest, PutMappingResponse> {

    private final MetaDataService metaDataService;

    @Inject public TransportPutMappingAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                             ThreadPool threadPool, MetaDataService metaDataService) {
        super(settings, transportService, clusterService, threadPool);
        this.metaDataService = metaDataService;
    }


    @Override protected String transportAction() {
        return TransportActions.Admin.Indices.Mapping.PUT;
    }

    @Override protected PutMappingRequest newRequest() {
        return new PutMappingRequest();
    }

    @Override protected PutMappingResponse newResponse() {
        return new PutMappingResponse();
    }

    @Override protected PutMappingResponse masterOperation(PutMappingRequest request) throws ElasticSearchException {
        ClusterState clusterState = clusterService.state();

        // update to concrete indices
        request.indices(clusterState.metaData().concreteIndices(request.indices()));
        final String[] indices = request.indices();

        MetaDataService.PutMappingResult result = metaDataService.putMapping(indices, request.type(), request.source(), request.ignoreConflicts(), request.timeout());
        return new PutMappingResponse(result.acknowledged());
    }
}