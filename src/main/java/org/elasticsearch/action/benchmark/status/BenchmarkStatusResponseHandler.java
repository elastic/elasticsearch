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

package org.elasticsearch.action.benchmark.status;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;

/**
 *
 */
public class BenchmarkStatusResponseHandler implements TransportResponseHandler<BenchmarkStatusNodeActionResponse> {

    private static final ESLogger logger = ESLoggerFactory.getLogger(BenchmarkStatusResponseHandler.class.getName());

    final String                          nodeId;
    final String                          benchmarkId;
    final BenchmarkStatusResponseListener listener;

    public BenchmarkStatusResponseHandler(String benchmarkId, String nodeId, BenchmarkStatusResponseListener listener) {
        this.benchmarkId = benchmarkId;
        this.nodeId      = nodeId;
        this.listener    = listener;
    }

    @Override
    public BenchmarkStatusNodeActionResponse newInstance() {
        return new BenchmarkStatusNodeActionResponse();
    }

    @Override
    public void handleResponse(BenchmarkStatusNodeActionResponse response) {
        logger.debug("benchmark [{}]: received results from [{}]", benchmarkId, response.nodeId);
        listener.onResponse(response.response());
    }

    @Override
    public void handleException(TransportException e) {
        logger.error("benchmark [{}]: failed to receive results", e, benchmarkId);
        listener.onFailure(e);
    }

    @Override
    public String executor() {
        return ThreadPool.Names.SAME;
    }
}
