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
import org.elasticsearch.transport.*;


/**
 * Transport action for benchmarks
 */
public class TransportBenchmarkAction extends TransportMasterNodeOperationAction<BenchmarkRequest, BenchmarkResponse> {

    private final BenchmarkService service;

    @Inject
    public TransportBenchmarkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    ThreadPool threadPool, BenchmarkService service) {
        super(settings, transportService, clusterService, threadPool);
        this.service = service;
    }

    @Override
    protected String transportAction() {
        return BenchmarkAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected BenchmarkRequest newRequest() {
        return new BenchmarkRequest();
    }

    @Override
    protected BenchmarkResponse newResponse() {
        return new BenchmarkResponse();
    }

    @Override
    protected void masterOperation(BenchmarkRequest request, ClusterState state, ActionListener<BenchmarkResponse> listener) throws ElasticsearchException {
        service.startBenchmark(request, listener);
    }
}
