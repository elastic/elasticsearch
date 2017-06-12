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
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.query.QuerySearchResult;

/**
 * A wrapper of search action listeners (search results) that unwraps the query
 * result to get the piggybacked queue size and service time EWMA, adding those
 * values to the coordinating nodes' {@code ResponseCollectorService}.
 */
public class ResponseListenerWrapper extends SearchActionListener<SearchPhaseResult> {

    private static final Logger logger = ESLoggerFactory.getLogger(ResponseListenerWrapper.class);

    private final SearchActionListener<SearchPhaseResult> listener;
    private final ResponseCollectorService collector;

    public ResponseListenerWrapper(SearchActionListener<SearchPhaseResult> listener,
                                   ResponseCollectorService collector) {
        super(listener.searchShardTarget, listener.requestIndex);
        this.listener = listener;
        this.collector = collector;
    }

    @Override
    protected void innerOnResponse(SearchPhaseResult response) {
        QuerySearchResult queryResult = response.queryResult();
        if (queryResult != null && collector != null) {
            long ewma = queryResult.serviceTimeEWMA();
            int queueSize = queryResult.nodeQueueSize();
            String nodeId = listener.searchShardTarget.getNodeId();
            if (nodeId != null) {
                collector.addQueueSize(nodeId, queueSize);
                // EWMA may be -1 if the query node doesn't support capturing it
                if (ewma > 0) {
                    collector.setServiceTime(nodeId, ewma);
                }
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] registering service time EWMA: {}, queue size: {}",
                            nodeId, TimeValue.timeValueNanos(ewma), queueSize);
                    logger.trace("queues sizes: [{}], service times: [{}], response times: [{}]",
                            collector.getAvgQueueSize(), collector.getAvgServiceTime(), collector.getAvgResponseTime());
                }
            }
        }
        listener.innerOnResponse(response);
    }

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }
}
