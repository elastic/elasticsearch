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

package org.elasticsearch.action.admin.indices.flush;

import com.google.inject.Inject;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.replication.TransportIndicesReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.settings.Settings;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportFlushAction extends TransportIndicesReplicationOperationAction<FlushRequest, FlushResponse, IndexFlushRequest, IndexFlushResponse, ShardFlushRequest, ShardFlushResponse> {

    @Inject public TransportFlushAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, TransportIndexFlushAction indexFlushAction) {
        super(settings, transportService, clusterService, threadPool, indexFlushAction);
    }

    @Override protected FlushRequest newRequestInstance() {
        return new FlushRequest();
    }

    @Override protected FlushResponse newResponseInstance(FlushRequest request, AtomicReferenceArray indexResponses) {
        FlushResponse response = new FlushResponse();
        for (int i = 0; i < indexResponses.length(); i++) {
            IndexFlushResponse indexFlushResponse = (IndexFlushResponse) indexResponses.get(i);
            if (indexFlushResponse != null) {
                response.indices().put(indexFlushResponse.index(), indexFlushResponse);
            }
        }
        return response;
    }

    @Override protected boolean accumulateExceptions() {
        return false;
    }

    @Override protected String transportAction() {
        return TransportActions.Admin.Indices.FLUSH;
    }

    @Override protected IndexFlushRequest newIndexRequestInstance(FlushRequest request, String index) {
        return new IndexFlushRequest(request, index);
    }
}