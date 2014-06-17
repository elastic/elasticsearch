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
package org.elasticsearch.action.bench;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for benchmark abort requests
 */
public class TransportAbortBenchmarkAction extends TransportMasterNodeOperationAction<AbortBenchmarkRequest, AbortBenchmarkResponse> {

    private final BenchmarkService service;

    @Inject
    public TransportAbortBenchmarkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, BenchmarkService service) {
        super(settings, transportService, clusterService, threadPool);
        this.service = service;
    }

    @Override
    protected String transportAction() {
        return AbortBenchmarkAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected AbortBenchmarkRequest newRequest() {
        return new AbortBenchmarkRequest();
    }

    @Override
    protected AbortBenchmarkResponse newResponse() {
        return new AbortBenchmarkResponse();
    }

    @Override
    protected void masterOperation(AbortBenchmarkRequest request, ClusterState state, final ActionListener<AbortBenchmarkResponse> listener) throws ElasticsearchException {
        service.abortBenchmark(request.benchmarkNames(), listener);
    }
}
