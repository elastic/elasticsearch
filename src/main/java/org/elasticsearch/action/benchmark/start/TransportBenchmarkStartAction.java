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
package org.elasticsearch.action.benchmark.start;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.benchmark.BenchmarkCoordinatorService;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;


/**
 * Transport action for starting benchmarks
 */
public class TransportBenchmarkStartAction extends TransportMasterNodeOperationAction<BenchmarkStartRequest, BenchmarkStartResponse> {

    private final BenchmarkCoordinatorService service;

    @Inject
    public TransportBenchmarkStartAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, BenchmarkCoordinatorService service, ActionFilters actionFilters) {
        super(settings, BenchmarkStartAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.service = service;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected BenchmarkStartRequest newRequest() {
        return new BenchmarkStartRequest();
    }

    @Override
    protected BenchmarkStartResponse newResponse() {
        return new BenchmarkStartResponse();
    }

    @Override
    protected void masterOperation(BenchmarkStartRequest request, ClusterState state, ActionListener<BenchmarkStartResponse> listener) throws ElasticsearchException {
        service.startBenchmark(request, listener);
    }
}
