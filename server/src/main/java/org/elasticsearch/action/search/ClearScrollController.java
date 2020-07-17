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
package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.TransportSearchHelper.parseScrollId;

final class ClearScrollController implements Runnable {
    private final DiscoveryNodes nodes;
    private final SearchTransportService searchTransportService;
    private final CountDown expectedOps;
    private final ActionListener<ClearScrollResponse> listener;
    private final AtomicBoolean hasFailed = new AtomicBoolean(false);
    private final AtomicInteger freedSearchContexts = new AtomicInteger(0);
    private final Logger logger;
    private final Runnable runner;

    ClearScrollController(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener, DiscoveryNodes nodes, Logger logger,
                          SearchTransportService searchTransportService) {
        this.nodes = nodes;
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.listener = listener;
        List<String> scrollIds = request.getScrollIds();
        final int expectedOps;
        if (scrollIds.size() == 1 && "_all".equals(scrollIds.get(0))) {
            expectedOps = nodes.getSize();
            runner = this::cleanAllScrolls;
        } else {
            List<ScrollIdForNode> parsedScrollIds = new ArrayList<>();
            for (String parsedScrollId : request.getScrollIds()) {
                ScrollIdForNode[] context = parseScrollId(parsedScrollId).getContext();
                for (ScrollIdForNode id : context) {
                    parsedScrollIds.add(id);
                }
            }
            if (parsedScrollIds.isEmpty()) {
                expectedOps = 0;
                runner = () -> listener.onResponse(new ClearScrollResponse(true, 0));
            } else {
                expectedOps = parsedScrollIds.size();
                runner = () -> cleanScrollIds(parsedScrollIds);
            }
        }
        this.expectedOps = new CountDown(expectedOps);

    }

    @Override
    public void run() {
        runner.run();
    }

    void cleanAllScrolls() {
        for (final DiscoveryNode node : nodes) {
            try {
                Transport.Connection connection = searchTransportService.getConnection(null, node);
                searchTransportService.sendClearAllScrollContexts(connection, new ActionListener<TransportResponse>() {
                    @Override
                    public void onResponse(TransportResponse response) {
                        onFreedContext(true);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onFailedFreedContext(e, node);
                    }
                });
            } catch (Exception e) {
                onFailedFreedContext(e, node);
            }
        }
    }

    void cleanScrollIds(List<ScrollIdForNode> parsedScrollIds) {
        SearchScrollAsyncAction.collectNodesAndRun(parsedScrollIds, nodes, searchTransportService, ActionListener.wrap(
            lookup -> {
                for (ScrollIdForNode target : parsedScrollIds) {
                    final DiscoveryNode node = lookup.apply(target.getClusterAlias(), target.getNode());
                    if (node == null) {
                        onFreedContext(false);
                    } else {
                        try {
                            Transport.Connection connection = searchTransportService.getConnection(target.getClusterAlias(), node);
                            searchTransportService.sendFreeContext(connection, target.getContextId(),
                                ActionListener.wrap(freed -> onFreedContext(freed.isFreed()), e -> onFailedFreedContext(e, node)));
                        } catch (Exception e) {
                            onFailedFreedContext(e, node);
                        }
                    }
                }
            }, listener::onFailure));
    }

    private void onFreedContext(boolean freed) {
        if (freed) {
            freedSearchContexts.incrementAndGet();
        }
        if (expectedOps.countDown()) {
            boolean succeeded = hasFailed.get() == false;
            listener.onResponse(new ClearScrollResponse(succeeded, freedSearchContexts.get()));
        }
    }

    private void onFailedFreedContext(Throwable e, DiscoveryNode node) {
        logger.warn(() -> new ParameterizedMessage("Clear SC failed on node[{}]", node), e);
        /*
         * We have to set the failure marker before we count down otherwise we can expose the failure marker before we have set it to a
         * racing thread successfully freeing a context. This would lead to that thread responding that the clear scroll succeeded.
         */
        hasFailed.set(true);
        if (expectedOps.countDown()) {
            listener.onResponse(new ClearScrollResponse(false, freedSearchContexts.get()));
        }
    }
}
