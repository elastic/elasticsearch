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
package org.elasticsearch.action.ingest;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicInteger;

public final class IngestProxyActionFilter implements ActionFilter {

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final AtomicInteger randomNodeGenerator = new AtomicInteger(Randomness.get().nextInt());

    @Inject
    public IngestProxyActionFilter(ClusterService clusterService, TransportService transportService) {
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override
    public <Request extends ActionRequest<Request>, Response extends ActionResponse> void apply(Task task, String action, Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        Action ingestAction;
        switch (action) {
            case IndexAction.NAME:
                ingestAction = IndexAction.INSTANCE;
                IndexRequest indexRequest = (IndexRequest) request;
                if (Strings.hasText(indexRequest.getPipeline())) {
                    forwardIngestRequest(ingestAction, request, listener);
                } else {
                    chain.proceed(task, action, request, listener);
                }
                break;
            case BulkAction.NAME:
                ingestAction = BulkAction.INSTANCE;
                BulkRequest bulkRequest = (BulkRequest) request;
                if (bulkRequest.hasIndexRequestsWithPipelines()) {
                    forwardIngestRequest(ingestAction, request, listener);
                } else {
                    chain.proceed(task, action, request, listener);
                }
                break;
            default:
                chain.proceed(task, action, request, listener);
                break;
        }
    }

    @SuppressWarnings("unchecked")
    private void forwardIngestRequest(Action<?, ?, ?> action, ActionRequest request, ActionListener<?> listener) {
        transportService.sendRequest(randomIngestNode(), action.name(), request, new ActionListenerResponseHandler(listener, action::newResponse));
    }

    @Override
    public <Response extends ActionResponse> void apply(String action, Response response, ActionListener<Response> listener, ActionFilterChain<?, Response> chain) {
        chain.proceed(action, response, listener);
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    private DiscoveryNode randomIngestNode() {
        assert clusterService.localNode().isIngestNode() == false;
        DiscoveryNodes nodes = clusterService.state().getNodes();
        DiscoveryNode[] ingestNodes = nodes.getIngestNodes().values().toArray(DiscoveryNode.class);
        if (ingestNodes.length == 0) {
            throw new IllegalStateException("There are no ingest nodes in this cluster, unable to forward request to an ingest node.");
        }

        int index = getNodeNumber();
        return ingestNodes[(index) % ingestNodes.length];
    }

    private int getNodeNumber() {
        int index = randomNodeGenerator.incrementAndGet();
        if (index < 0) {
            index = 0;
            randomNodeGenerator.set(0);
        }
        return index;
    }
}
