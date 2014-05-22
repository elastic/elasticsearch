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

package org.elasticsearch.action.benchmark;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.benchmark.abort.*;
import org.elasticsearch.action.benchmark.exception.BenchmarkIdConflictException;
import org.elasticsearch.action.benchmark.pause.BenchmarkPauseRequest;
import org.elasticsearch.action.benchmark.resume.BenchmarkResumeRequest;
import org.elasticsearch.action.benchmark.start.BenchmarkStartRequest;
import org.elasticsearch.action.benchmark.start.BenchmarkStartResponse;
import org.elasticsearch.action.benchmark.status.BenchmarkStatusResponseHandler;
import org.elasticsearch.action.benchmark.status.BenchmarkStatusResponseListener;
import org.elasticsearch.action.benchmark.status.BenchmarkStatusTransportRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TimeoutClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.BenchmarkMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages cluster metadata state for benchmarks
 */
public class BenchmarkStateManager {

    private static final ESLogger logger = ESLoggerFactory.getLogger(BenchmarkStateManager.class.getName());

    private final ClusterService   clusterService;
    private final ThreadPool       threadPool;
    private final BenchmarkUtility utility;
    private final TransportService transportService;

    @Inject
    public BenchmarkStateManager(final ClusterService clusterService, final ThreadPool threadPool,
                                 final TransportService transportService, final BenchmarkUtility utility) {
        this.clusterService   = clusterService;
        this.threadPool       = threadPool;
        this.transportService = transportService;
        this.utility          = utility;
    }

    public void init(final BenchmarkStartRequest request, final ActionListener listener) {

        final String cause = "benchmark-start-request (" + request.benchmarkId() + ")";

        clusterService.submitStateUpdateTask(cause, new TimeoutClusterStateUpdateTask() {

            @Override
            public TimeValue timeout() { return request.masterNodeTimeout(); }

            @Override
            public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) { }

            @Override
            public ClusterState execute(ClusterState state) throws Exception {

                final BenchmarkMetaData meta = state.metaData().custom(BenchmarkMetaData.TYPE);
                if (BenchmarkUtility.exists(request.benchmarkId(), meta)) {
                    throw new BenchmarkIdConflictException("benchmark [" + request.benchmarkId() + "]: already exists");
                }

                final ImmutableList.Builder<BenchmarkMetaData.Entry> builder = ImmutableList.builder();
                if (meta != null) {
                    for (BenchmarkMetaData.Entry entry : meta.entries()) {
                        builder.add(entry);
                    }
                }

                // Assign nodes on which to execute the benchmark
                final BenchmarkMetaData.Entry entry = new BenchmarkMetaData.Entry(request.benchmarkId());
                final List<DiscoveryNode> nodes = utility.executors(request.numExecutorNodes());
                for (DiscoveryNode node : nodes) {
                    entry.nodeStateMap().put(node.id(), BenchmarkMetaData.Entry.NodeState.INITIALIZING);
                }

                // Add benchmark to cluster metadata
                builder.add(entry);
                final MetaData.Builder metabuilder = MetaData.builder(state.metaData());
                metabuilder.putCustom(BenchmarkMetaData.TYPE, new BenchmarkMetaData(builder.build()));

                // Notify caller that everything is OK
                listener.onResponse(null);

                return ClusterState.builder(state).metaData(metabuilder).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("failed to initialize benchmark state [{}] ([{}])", t, request.benchmarkId(), source);
                listener.onFailure(t);
            }
        });
    }

    private interface Eligibility {
        boolean eligible(final BenchmarkMetaData.Entry entry);
    }

    private static class BaseUpdateTask implements TimeoutClusterStateUpdateTask {

        final TimeValue                timeValue;
        final ActionListener<String[]> listener;
        final String[]                 patterns;
        final Eligibility              eligibility;
        final BenchmarkMetaData.State  target;
        final List<String>             found = new ArrayList<>();

        BaseUpdateTask(final String[] patterns, final TimeValue timeValue, final ActionListener<String[]> listener,
                       final BenchmarkMetaData.State target, final Eligibility eligibility) {
            this.timeValue   = timeValue;
            this.patterns    = patterns;
            this.listener    = listener;
            this.target      = target;
            this.eligibility = eligibility;
        }

        @Override
        public TimeValue timeout() {
            return timeValue;
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            listener.onResponse(found.toArray(new String[found.size()]));
        }

        @Override
        public ClusterState execute(ClusterState state) throws Exception {

            final BenchmarkMetaData meta = state.metaData().custom(BenchmarkMetaData.TYPE);
            if (meta == null || meta.entries().size() == 0) {
                return state;
            }

            final ImmutableList.Builder<BenchmarkMetaData.Entry> builder = ImmutableList.builder();

            for (BenchmarkMetaData.Entry entry : meta.entries()) {

                if (!eligibility.eligible(entry)) {
                    builder.add(entry);
                } else if (Regex.simpleMatch(patterns, entry.benchmarkId())) {
                    builder.add(new BenchmarkMetaData.Entry(entry.benchmarkId(), target, entry.nodeStateMap()));
                    found.add(entry.benchmarkId());
                } else {
                    builder.add(entry);
                }
            }

            if (found.size() > 0) {
                final MetaData.Builder metabuilder = MetaData.builder(state.metaData());
                metabuilder.putCustom(BenchmarkMetaData.TYPE, new BenchmarkMetaData(builder.build()));
                return ClusterState.builder(state).metaData(metabuilder).build();
            } else {
                return state;
            }
        }

        @Override
        public void onFailure(String source, Throwable t) {
            logger.error("failed to set benchmark state to {} [{}] ([{}])", t, target, patterns, source);
            listener.onFailure(t);
        }
    }

    public void pause(final BenchmarkPauseRequest request, final ActionListener<String[]> listener) {

        final String cause = "benchmark-pause-request (" + Joiner.on(",").join(request.benchmarkIdPatterns()) + ")";

        clusterService.submitStateUpdateTask(cause,
                new BaseUpdateTask(request.benchmarkIdPatterns(), request.masterNodeTimeout(), listener,
                        BenchmarkMetaData.State.PAUSED,
                        new Eligibility() {
                            @Override
                            public boolean eligible(BenchmarkMetaData.Entry entry) {
                                return !(entry.state() == BenchmarkMetaData.State.INITIALIZING ||
                                         entry.state() == BenchmarkMetaData.State.COMPLETED ||
                                         entry.state() == BenchmarkMetaData.State.ABORTED ||
                                         entry.state() == BenchmarkMetaData.State.FAILED);
                            }
                        }));
    }

    public void resume(final BenchmarkResumeRequest request, final ActionListener<String[]> listener) {

        final String cause = "benchmark-resume-request (" + Joiner.on(",").join(request.benchmarkIdPatterns()) + ")";

        clusterService.submitStateUpdateTask(cause,
                new BaseUpdateTask(request.benchmarkIdPatterns(), request.masterNodeTimeout(), listener,
                        BenchmarkMetaData.State.RESUMING,
                        new Eligibility() {
                            @Override
                            public boolean eligible(BenchmarkMetaData.Entry entry) {
                                return entry.state() == BenchmarkMetaData.State.PAUSED;
                            }
                        }));
    }

    public void abort(final BenchmarkAbortRequest request, final ActionListener<String[]> listener) {

        final String cause = "benchmark-abort-request (" + Joiner.on(",").join(request.benchmarkIdPatterns()) + ")";

        clusterService.submitStateUpdateTask(cause,
                new BaseUpdateTask(request.benchmarkIdPatterns(), request.masterNodeTimeout(), listener,
                        BenchmarkMetaData.State.ABORTED,
                        new Eligibility() {
                            @Override
                            public boolean eligible(BenchmarkMetaData.Entry entry) {
                                return !(entry.state() == BenchmarkMetaData.State.COMPLETED ||
                                         entry.state() == BenchmarkMetaData.State.ABORTED ||
                                         entry.state() == BenchmarkMetaData.State.FAILED);
                            }
                        }));
    }


    public void update(final String benchmarkId, final BenchmarkMetaData.State benchmarkState,
                       final BenchmarkMetaData.Entry.NodeState nodeState, final ActionListener listener) {

        final String cause = "benchmark-update-state (" + benchmarkId + ":" + benchmarkState + ")";

        clusterService.submitStateUpdateTask(cause, new TimeoutClusterStateUpdateTask() {
            @Override
            public TimeValue timeout() {
                return MasterNodeOperationRequest.DEFAULT_MASTER_NODE_TIMEOUT;
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) { }

            @Override
            public ClusterState execute(ClusterState clusterState) throws Exception {

                final BenchmarkMetaData meta = clusterState.metaData().custom(BenchmarkMetaData.TYPE);
                final ImmutableList.Builder<BenchmarkMetaData.Entry> builder = ImmutableList.builder();

                for (BenchmarkMetaData.Entry entry : meta.entries()) {
                    if (entry.benchmarkId().equals(benchmarkId)) {
                        Map<String, BenchmarkMetaData.Entry.NodeState> map = entry.nodeStateMap();
                        if (nodeState != null) {
                            map = new HashMap<>();
                            for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
                                map.put(e.getKey(), nodeState);
                            }
                        }
                        builder.add(new BenchmarkMetaData.Entry(entry.benchmarkId(), benchmarkState, map));
                    } else {
                        builder.add(entry);
                    }
                }

                final MetaData.Builder metabuilder = MetaData.builder(clusterState.metaData());
                metabuilder.putCustom(BenchmarkMetaData.TYPE, new BenchmarkMetaData(builder.build()));

                return ClusterState.builder(clusterState).metaData(metabuilder).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("failed to update benchmark state [{}] ([{}])", t, benchmarkId, source);
                listener.onFailure(t);
            }
        });
    }

    public BenchmarkStartResponse status(final BenchmarkMetaData.Entry entry) throws InterruptedException {

        final BenchmarkStatusResponseListener listener = new BenchmarkStatusResponseListener(entry.nodeStateMap().size());
        final List<String>                    errors   = new ArrayList<>();

        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {

            final DiscoveryNode node = clusterService.state().nodes().get(e.getKey());
            if (node == null) {
                logger.warn("benchmark [{}]: node [{}] unavailable", entry.benchmarkId(), e.getKey());
                errors.add("benchmark [" + entry.benchmarkId() + "]: node [" + e.getKey() + "] unavailable");
                listener.countdown();
                continue;
            }

            final BenchmarkStatusResponseHandler handler = new BenchmarkStatusResponseHandler(entry.benchmarkId(), node.id(), listener);

            threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                @Override
                public void run() {
                    logger.debug("benchmark [{}]: requesting status from [{}]", entry.benchmarkId(), node.id());
                    transportService.sendRequest(
                            node,
                            BenchmarkExecutorService.BenchmarkStatusRequestHandler.ACTION,
                            new BenchmarkStatusTransportRequest(entry.benchmarkId(), node.id()),
                            handler);
                }
            });
        }

        // Block pending response by all executors
        listener.awaitCompletion();
        listener.response().errors(errors);

        return listener.response();
    }

    public void clear(final String benchmarkId, final ActionListener listener) {

        final String cause = "benchmark-clear-state (" + benchmarkId + ")";

        clusterService.submitStateUpdateTask(cause, new TimeoutClusterStateUpdateTask() {
            @Override
            public TimeValue timeout() {
                return MasterNodeOperationRequest.DEFAULT_MASTER_NODE_TIMEOUT;
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.debug("benchmark [{}]: cleared from cluster state [{}]", benchmarkId);
                listener.onResponse(null);
            }

            @Override
            public ClusterState execute(ClusterState state) throws Exception {

                final BenchmarkMetaData                              meta = state.metaData().custom(BenchmarkMetaData.TYPE);
                final ImmutableList.Builder<BenchmarkMetaData.Entry> builder = ImmutableList.builder();

                for (BenchmarkMetaData.Entry entry : meta.entries()) {
                    if (!entry.benchmarkId().equals(benchmarkId)) {
                        builder.add(entry);
                    }
                }

                final MetaData.Builder metabuilder = MetaData.builder(state.metaData());
                metabuilder.putCustom(BenchmarkMetaData.TYPE, new BenchmarkMetaData((builder.build())));

                return ClusterState.builder(state).metaData(metabuilder).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("failed to clear benchmark state [{}] ([{}])", t, benchmarkId, source);
                listener.onFailure(t);
            }
        });
    }
}
