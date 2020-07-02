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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.TransportSearchHelper.parseScrollId;

final class ClearSearchContextController implements Runnable {
    private final DiscoveryNodes nodes;
    private final SearchTransportService searchTransportService;
    private final CountDown expectedOps;
    private final ActionListener<? super CloseSearchContextResponse> listener;
    private final AtomicBoolean hasFailed = new AtomicBoolean(false);
    private final AtomicInteger freedSearchContexts = new AtomicInteger(0);
    private final Logger logger;
    private final Runnable runner;

    ClearSearchContextController(ClearScrollRequest clearScrollRequest, ActionListener<ClearScrollResponse> listener,
                                 DiscoveryNodes nodes, Logger logger, SearchTransportService searchTransportService) {
        this.nodes = nodes;
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.listener = listener;
        final int expectedOps;
        final List<String> scrollIds = clearScrollRequest.getScrollIds();
        if (scrollIds.size() == 1 && "_all".equals(scrollIds.get(0))) {
            expectedOps = nodes.getSize();
            runner = this::cleanAllScrolls;
        } else {
            List<SearchContextIdForNode> contexts = new ArrayList<>();
            for (String scrollId : scrollIds) {
                contexts.addAll(Arrays.asList(parseScrollId(scrollId).getContext()));
            }
            expectedOps = contexts.size();
            runner = () -> cleanReaderIds(contexts);
        }
        this.expectedOps = new CountDown(expectedOps);
    }

    ClearSearchContextController(CloseSearchContextRequest closeSearchContextRequest, ActionListener<CloseSearchContextResponse> listener,
                                 DiscoveryNodes nodes, Logger logger, SearchTransportService searchTransportService) {
        this.nodes = nodes;
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.listener = listener;
        final SearchContextId context = SearchContextId.decode(closeSearchContextRequest.getId());
        expectedOps = new CountDown(context.shards().size());
        runner = () -> cleanReaderIds(context.shards().values());
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

    void cleanReaderIds(Collection<SearchContextIdForNode> readerIds) {
        if (readerIds.isEmpty()) {
            listener.onResponse(new CloseSearchContextResponse(true, 0));
            return;
        }
        SearchScrollAsyncAction.collectNodesAndRun(readerIds, nodes, searchTransportService, ActionListener.wrap(
            lookup -> {
                for (SearchContextIdForNode target : readerIds) {
                    final DiscoveryNode node = lookup.apply(target.getClusterAlias(), target.getNode());
                    if (node == null) {
                        onFreedContext(false);
                    } else {
                        try {
                            Transport.Connection connection = searchTransportService.getConnection(target.getClusterAlias(), node);
                            searchTransportService.sendFreeContext(connection, target.getSearchContextId(),
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
            listener.onResponse(new CloseSearchContextResponse(succeeded, freedSearchContexts.get()));
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
            listener.onResponse(new CloseSearchContextResponse(false, freedSearchContexts.get()));
        }
    }
}
