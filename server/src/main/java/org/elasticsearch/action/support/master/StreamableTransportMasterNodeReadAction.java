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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Supplier;

@Deprecated
public abstract class StreamableTransportMasterNodeReadAction<Request extends MasterNodeReadRequest<Request>, Response extends ActionResponse>
    extends TransportMasterNodeReadAction<Request, Response> {

    protected StreamableTransportMasterNodeReadAction(String actionName, TransportService transportService,
                                                      ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                                      IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request) {
        super(actionName, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, request);
    }

    protected StreamableTransportMasterNodeReadAction(String actionName, TransportService transportService,
                                                      ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                                      Writeable.Reader<Request> request,
                                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(actionName, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
    }

    protected StreamableTransportMasterNodeReadAction(String actionName, boolean checkSizeLimit, TransportService transportService,
                                                      ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                                      IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request) {
        super(actionName, checkSizeLimit, transportService, clusterService, threadPool,
            actionFilters, indexNameExpressionResolver, request);
    }

    protected StreamableTransportMasterNodeReadAction(String actionName, boolean checkSizeLimit, TransportService transportService,
                                                      ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                                      Writeable.Reader<Request> request,
                                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(actionName, checkSizeLimit, transportService, clusterService, threadPool,
            actionFilters, request, indexNameExpressionResolver);
    }

    /**
     * @return a new response instance. Typically this is used for serialization using the
     *         {@link org.elasticsearch.common.io.stream.Streamable#readFrom(StreamInput)} method.
     */
    protected abstract Response newResponse();

    @Override
    protected final Response read(StreamInput in) throws IOException {
        Response response = newResponse();
        response.readFrom(in);
        return response;
    }
}
