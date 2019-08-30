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

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * A base class for read operations that needs to be performed on the master node.
 * Can also be executed on the local node if needed.
 */
public abstract class TransportMasterNodeReadAction<Request extends MasterNodeReadRequest<Request>, Response extends ActionResponse>
        extends TransportMasterNodeAction<Request, Response> {

    protected TransportMasterNodeReadAction(String actionName, TransportService transportService,
                                            ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                            Writeable.Reader<Request> request, IndexNameExpressionResolver indexNameExpressionResolver) {
        this(actionName, true, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
    }

    protected TransportMasterNodeReadAction(String actionName, boolean checkSizeLimit, TransportService transportService,
                                            ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                            Writeable.Reader<Request> request, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(actionName, checkSizeLimit, transportService, clusterService, threadPool, actionFilters, request,
            indexNameExpressionResolver);
    }

    @Override
    protected final boolean localExecute(Request request) {
        return request.local();
    }
}
