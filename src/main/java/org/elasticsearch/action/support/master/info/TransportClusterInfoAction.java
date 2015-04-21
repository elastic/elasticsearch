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
package org.elasticsearch.action.support.master.info;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 */
public abstract class TransportClusterInfoAction<Request extends ClusterInfoRequest, Response extends ActionResponse> extends TransportMasterNodeReadOperationAction<Request, Response> {

    public TransportClusterInfoAction(Settings settings, String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters, Class<Request> request) {
        super(settings, actionName, transportService, clusterService, threadPool, actionFilters, request);
    }

    @Override
    protected String executor() {
        // read operation, lightweight...
        return ThreadPool.Names.SAME;
    }

    @Override
    protected final void masterOperation(final Request request, final ClusterState state, final ActionListener<Response> listener) throws ElasticsearchException {
        String[] concreteIndices = state.metaData().concreteIndices(request.indicesOptions(), request.indices());
        doMasterOperation(request, concreteIndices, state, listener);
    }

    protected abstract void doMasterOperation(Request request, String[] concreteIndices, ClusterState state, final ActionListener<Response> listener) throws ElasticsearchException;
}
