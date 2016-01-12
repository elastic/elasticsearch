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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.ingest.IngestModule;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
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
    public void apply(Task task, String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
        Action ingestAction = null;
        boolean isIngestRequest = false;
        if (IndexAction.NAME.equals(action)) {
            ingestAction = IndexAction.INSTANCE;
            assert request instanceof IndexRequest;
            IndexRequest indexRequest = (IndexRequest) request;
            isIngestRequest = Strings.hasText(indexRequest.pipeline());
        } else if (BulkAction.NAME.equals(action)) {
            ingestAction = BulkAction.INSTANCE;
            assert request instanceof BulkRequest;
            BulkRequest bulkRequest = (BulkRequest) request;
            for (ActionRequest actionRequest : bulkRequest.requests()) {
                if (actionRequest instanceof IndexRequest) {
                    IndexRequest indexRequest = (IndexRequest) actionRequest;
                    if (Strings.hasText(indexRequest.pipeline())) {
                        isIngestRequest = true;
                        break;
                    }
                }
            }
        }

        if (isIngestRequest) {
            assert ingestAction != null;
            forwardIngestRequest(ingestAction, request, listener);
            return;
        }
        chain.proceed(task, action, request, listener);
    }

    private void forwardIngestRequest(Action action, ActionRequest request, ActionListener listener) {
        transportService.sendRequest(randomIngestNode(), action.name(), request, new TransportResponseHandler<TransportResponse>() {
            @Override
            public TransportResponse newInstance() {
                return action.newResponse();
            }

            @Override
            @SuppressWarnings("unchecked")
            public void handleResponse(TransportResponse response) {
                listener.onResponse(response);
            }

            @Override
            public void handleException(TransportException exp) {
                listener.onFailure(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        });
    }

    @Override
    public void apply(String action, ActionResponse response, ActionListener listener, ActionFilterChain chain) {
        chain.proceed(action, response, listener);
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    private DiscoveryNode randomIngestNode() {
        assert IngestModule.isIngestEnabled(clusterService.localNode().attributes()) == false;
        List<DiscoveryNode> ingestNodes = new ArrayList<>();
        for (DiscoveryNode node : clusterService.state().nodes()) {
            if (IngestModule.isIngestEnabled(node.getAttributes())) {
                ingestNodes.add(node);
            }
        }

        if (ingestNodes.isEmpty()) {
            throw new IllegalStateException("There are no ingest nodes in this cluster, unable to forward request to an ingest node.");
        }

        int index = getNodeNumber();
        return ingestNodes.get((index) % ingestNodes.size());
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
