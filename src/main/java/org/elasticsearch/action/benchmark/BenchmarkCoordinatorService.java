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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.benchmark.abort.*;
import org.elasticsearch.action.benchmark.pause.*;
import org.elasticsearch.action.benchmark.resume.*;
import org.elasticsearch.action.benchmark.start.*;
import org.elasticsearch.action.benchmark.status.*;
import org.elasticsearch.action.benchmark.exception.*;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Service component for running benchmarks.
 */
public class BenchmarkCoordinatorService extends AbstractBenchmarkService<BenchmarkCoordinatorService> {

    protected final BenchmarkStateManager manager;
    protected final Map<String, InternalCoordinatorState> benchmarks = new ConcurrentHashMap<>();

    /**
     * Constructs a service component for running benchmarks
     */
    @Inject
    public BenchmarkCoordinatorService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                       TransportService transportService, BenchmarkStateManager manager,
                                       BenchmarkUtility utility) {

        super(settings, clusterService, transportService, threadPool, utility);

        this.manager = manager;

        transportService.registerHandler(NodeStateUpdateRequestHandler.ACTION, new NodeStateUpdateRequestHandler());
        transportService.registerHandler(BenchmarkDefinitionRequestHandler.ACTION, new BenchmarkDefinitionRequestHandler());
    }

    @Override
    protected void doStart() throws ElasticsearchException { }
    @Override
    protected void doStop() throws ElasticsearchException { }
    @Override
    protected void doClose() throws ElasticsearchException { }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        // XXX - Need to detect dropped node here and remove from internal state where appropriate

        final BenchmarkMetaData meta = event.state().metaData().custom(BenchmarkMetaData.TYPE);
        final BenchmarkMetaData prev = event.previousState().metaData().custom(BenchmarkMetaData.TYPE);

        if (!isMasterNode() || !event.metaDataChanged() || meta == null || meta.entries().size() == 0) {
            return;
        }

        for (final BenchmarkMetaData.Entry entry : BenchmarkMetaData.delta(prev, meta)) {

            log(entry);
            final InternalCoordinatorState ics = benchmarks.get(entry.benchmarkId());
            if (ics == null) {
                logger.warn("unknown benchmark in cluster metadata [{}]", entry.benchmarkId());
                continue;
            }

            switch (entry.state()) {
                case INITIALIZING:
                    if (allNodesReady(entry)) {
                        // Once all executors have initialized and reported 'ready', we can update the benchmark's
                        // top-level state, thereby signalling to the executors that it's okay to begin execution.
                        if (ics.canStartRunning()) {
                            ics.onReady.onStateChange(entry);
                        }
                    }
                    break;
                case RUNNING:
                    if (allNodesFinished(entry)) {
                        // Once all executors have completed, successfully or otherwise, we can fetch the benchmark's
                        // results from each executor node, consolidate into a single top-level result, and update
                        // the benchmark's top-level state.
                        if (ics.canStopRunning()) {
                            ics.onFinished.onStateChange(entry);
                        }
                    }
                    break;
                case RESUMING:
                    if (allNodesRunning(entry)) {
                        if (ics.canResumeRunning()) {
                            assert ics.onResumed != null;
                            ics.onResumed.onStateChange(entry);
                        }
                    }
                    break;
                case PAUSED:
                    if (allNodesPaused(entry)) {
                        if (ics.canPauseRunning()) {
                            assert ics.onPaused != null;
                            ics.onPaused.onStateChange(entry);
                        }
                    }
                    break;
                case COMPLETED:
                    if (ics.canComplete()) {
                        ics.onComplete.onStateChange(entry);
                    }
                    break;
                case FAILED:

                    // XXX - Finish
                    throw new UnsupportedOperationException("Unimplemented");

                case ABORTED:
                    if (allNodesAborted(entry)) {
                        assert ics.onAbort != null;
                        ics.onAbort.onStateChange(entry);
                    }
                    break;
                default:
                    throw new BenchmarkIllegalStateException("benchmark [" + entry.benchmarkId() + "]: illegal state [" + entry.state() + "]");
            }
        }
    }

    /* ** Public API Methods ** */

    public void startBenchmark(final BenchmarkStartRequest request,
                               final ActionListener<BenchmarkStartResponse> listener) {

        preconditions(request.numExecutorNodes());

        manager.init(request, new ActionListener() {

            @Override
            public void onResponse(Object o) {

                assert null == benchmarks.get(request.benchmarkId());

                final InternalCoordinatorState ics = new InternalCoordinatorState(request, listener);

                ics.onReady    = new OnReadyStateChangeListener(ics);
                ics.onFinished = new OnFinishedStateChangeListener(ics);
                ics.onComplete = new OnCompleteStateChangeListener(ics);

                benchmarks.put(request.benchmarkId(), ics);
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

    public void listBenchmarks(final BenchmarkStatusRequest request, 
                               final ActionListener<BenchmarkStatusResponses> listener) {

        final BenchmarkMetaData meta = clusterService.state().metaData().custom(BenchmarkMetaData.TYPE);

        if (utility.executors(1).size() == 0 || meta == null || meta.entries().size() == 0) {
            listener.onResponse(new BenchmarkStatusResponses());
            return;
        }

        final BenchmarkStatusResponses responses = new BenchmarkStatusResponses();

        for (final BenchmarkMetaData.Entry entry : meta.entries()) {

            if (request.benchmarkIdPatterns() == null ||
                request.benchmarkIdPatterns().length == 0 ||
                Regex.simpleMatch(request.benchmarkIdPatterns(), entry.benchmarkId())) {

                try {
                    responses.add(manager.status(entry));
                } catch (Throwable t) {
                    logger.error("benchmark [{}]: failed to read status", t, entry.benchmarkId());
                    listener.onFailure(t);
                    return;
                }
            }
        }

        listener.onResponse(responses);
    }

    public void pauseBenchmark(final BenchmarkPauseRequest request,
                               final ActionListener<BenchmarkPauseResponse> listener) {

        preconditions(1);

        manager.pause(request, new ActionListener<String[]>() {

            @Override
            public void onResponse(String[] benchmarkIds) {
                if (benchmarkIds == null || benchmarkIds.length == 0) {
                    listener.onFailure(
                            new BenchmarkMissingException(
                                    "No benchmarks found matching: [" + Joiner.on(",").join(request.benchmarkIdPatterns()) + "]"));
                } else {
                    for (final String benchmarkId : benchmarkIds) {
                        final InternalCoordinatorState ics = benchmarks.get(benchmarkId);
                        if (ics == null) {
                            throw new BenchmarkIllegalStateException("benchmark [" + benchmarkId + "]: missing internal state");
                        }

                        ics.onPaused = new OnPausedStateChangeListener(ics, listener);
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
    }

    public void resumeBenchmark(final BenchmarkResumeRequest request,
                                final ActionListener<BenchmarkResumeResponse> listener) {

        preconditions(1);

        manager.resume(request, new ActionListener<String[]>() {

            @Override
            public void onResponse(String[] benchmarkIds) {
                if (benchmarkIds == null || benchmarkIds.length == 0) {
                    listener.onFailure(new BenchmarkMissingException(
                            "No benchmarks found matching: [" + Joiner.on(",").join(request.benchmarkIdPatterns()) + "]"));
                } else {
                    for (final String benchmarkId : benchmarkIds) {
                        final InternalCoordinatorState ics = benchmarks.get(benchmarkId);
                        if (ics == null) {
                            throw new BenchmarkIllegalStateException("benchmark [" + benchmarkId + "]: missing internal state");
                        }

                        ics.onResumed = new OnResumedStateChangeListener(ics, listener);
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
    }

    public void abortBenchmark(final BenchmarkAbortRequest request,
                               final ActionListener<BenchmarkAbortResponse> listener) {

        preconditions(1);

        manager.abort(request, new ActionListener<String[]>() {

            @Override
            public void onResponse(String[] benchmarkIds) {
                if (benchmarkIds == null || benchmarkIds.length == 0) {
                    listener.onFailure(new BenchmarkMissingException(
                            "No benchmarks found matching: [" + Joiner.on(",").join(request.benchmarkIdPatterns()) + "]"));
                } else {
                    for (final String benchmarkId : benchmarkIds) {
                        final InternalCoordinatorState ics = benchmarks.get(benchmarkId);
                        if (ics == null) {
                            throw new BenchmarkIllegalStateException("benchmark [" + benchmarkId + "]: missing internal state");
                        }

                        ics.onAbort = new OnAbortStateChangeListener(ics, listener);
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
    }

    /* ** State Change Listeners ** */

    private class OnReadyStateChangeListener extends StateChangeListener {

        final InternalCoordinatorState ics;

        OnReadyStateChangeListener(InternalCoordinatorState ics) {
            this.ics = ics;
        }

        @Override
        void onStateChange(final BenchmarkMetaData.Entry entry) {

            manager.update(ics.benchmarkId, BenchmarkMetaData.State.RUNNING, BenchmarkMetaData.Entry.NodeState.RUNNING,
                    new ActionListener() {
                        @Override
                        public void onResponse(Object o) { /* no-op */ }

                        @Override
                        public void onFailure(Throwable e) {
                            ics.onFailure(e);
                        }
                    });
        }
    }

    private class OnFinishedStateChangeListener extends StateChangeListener {

        final InternalCoordinatorState ics;

        OnFinishedStateChangeListener(InternalCoordinatorState ics) {
            this.ics = ics;
        }

        @Override
        void onStateChange(final BenchmarkMetaData.Entry entry) {

            try {
                ics.response = manager.status(entry);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("benchmark [{}]: failed to read status", e, entry.benchmarkId());
                ics.onFailure(e);
            }

            manager.update(ics.benchmarkId, BenchmarkMetaData.State.COMPLETED, BenchmarkMetaData.Entry.NodeState.COMPLETED,
                    new ActionListener() {
                        @Override
                        public void onResponse(Object o) { /* no-op */ }

                        @Override
                        public void onFailure(Throwable e) {
                            ics.onFailure(e);
                        }
                    });
        }
    }

    private class OnCompleteStateChangeListener extends StateChangeListener {

        final InternalCoordinatorState ics;

        OnCompleteStateChangeListener(final InternalCoordinatorState ics) {
            this.ics = ics;
        }

        @Override
        void onStateChange(final BenchmarkMetaData.Entry entry) {

             manager.clear(ics.benchmarkId, new ActionListener() {
                @Override
                public void onResponse(Object o) {
                    benchmarks.remove(ics.benchmarkId);
                    ics.onResponse();
                }

                @Override
                public void onFailure(Throwable e) {
                    benchmarks.remove(ics.benchmarkId);
                    ics.onFailure(e);
                }
            });
        }
    }

    private class OnAbortStateChangeListener extends StateChangeListener {

        final InternalCoordinatorState               ics;
        final ActionListener<BenchmarkAbortResponse> listener;

        OnAbortStateChangeListener(final InternalCoordinatorState ics, final ActionListener<BenchmarkAbortResponse> listener) {
            this.ics      = ics;
            this.listener = listener;
        }

        @Override
        void onStateChange(BenchmarkMetaData.Entry entry) {

            final BenchmarkAbortResponse response = new BenchmarkAbortResponse(entry.benchmarkId());

            for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
                response.addNodeResponse(e.getKey(), e.getValue());
            }

            listener.onResponse(response);

            // Initiate completion sequence; send partial results back to original caller
            ics.onFinished.onStateChange(entry);
        }
    }

    private class OnPausedStateChangeListener extends StateChangeListener {

        final InternalCoordinatorState               ics;
        final ActionListener<BenchmarkPauseResponse> listener;

        OnPausedStateChangeListener(final InternalCoordinatorState ics, final ActionListener<BenchmarkPauseResponse> listener) {
            this.ics      = ics;
            this.listener = listener;
        }

        @Override
        void onStateChange(BenchmarkMetaData.Entry entry) {

            final BenchmarkPauseResponse response = new BenchmarkPauseResponse(entry.benchmarkId());

            for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
                response.addNodeResponse(e.getKey(), e.getValue());
            }

            listener.onResponse(response);
        }
    }

    private class OnResumedStateChangeListener extends StateChangeListener {

        final InternalCoordinatorState                ics;
        final ActionListener<BenchmarkResumeResponse> listener;

        OnResumedStateChangeListener(final InternalCoordinatorState ics, final ActionListener<BenchmarkResumeResponse> listener) {
            this.ics      = ics;
            this.listener = listener;
        }

        @Override
        void onStateChange(BenchmarkMetaData.Entry entry) {

            final BenchmarkResumeResponse response = new BenchmarkResumeResponse(entry.benchmarkId());

            for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
                response.addNodeResponse(e.getKey(), e.getValue());
            }

            listener.onResponse(response);

            manager.update(ics.benchmarkId, BenchmarkMetaData.State.RUNNING, BenchmarkMetaData.Entry.NodeState.RUNNING,
                    new ActionListener() {
                        @Override
                        public void onResponse(Object o) { /* no-op */ }

                        @Override
                        public void onFailure(Throwable e) {
                            ics.onFailure(e);
                        }
                    });
        }
    }

    /* ** Utilities ** */

    protected class InternalCoordinatorState {

        final String                benchmarkId;
        final BenchmarkStartRequest request;
        BenchmarkStartResponse      response;

        AtomicBoolean running  = new AtomicBoolean(false);
        AtomicBoolean complete = new AtomicBoolean(false);
        AtomicBoolean paused   = new AtomicBoolean(false);

        ActionListener<BenchmarkStartResponse> listener;
        final Object listenerLock = new Object();

        OnReadyStateChangeListener    onReady;
        OnFinishedStateChangeListener onFinished;
        OnCompleteStateChangeListener onComplete;
        OnPausedStateChangeListener   onPaused;
        OnResumedStateChangeListener  onResumed;
        OnAbortStateChangeListener    onAbort;

        InternalCoordinatorState(BenchmarkStartRequest request, final ActionListener<BenchmarkStartResponse> listener) {
            this.benchmarkId = request.benchmarkId();
            this.request     = request;
            this.listener    = listener;
        }

        boolean canStartRunning() {
            return running.compareAndSet(false, true);
        }

        boolean canStopRunning() {
            return running.compareAndSet(true, false);
        }

        boolean canPauseRunning() {
            return paused.compareAndSet(false, true);
        }

        boolean canResumeRunning() {
            return paused.compareAndSet(true, false);
        }

        boolean canComplete() {
            return complete.compareAndSet(false, true);
        }

        void onFailure(Throwable t) {
            synchronized (listenerLock) {
                if (listener == null) {
                    logger.warn("benchmark [{}]: attempted redundant response [{}]", benchmarkId, t.getMessage());
                } else {
                    try {
                        listener.onFailure(t);
                    } finally {
                        listener = null;
                    }
                }
            }
        }

        void onResponse() {
            synchronized (listenerLock) {
                if (listener == null) {
                    logger.warn("benchmark [{}]: attempted redundant response [{}]", benchmarkId);
                } else {
                    try {
                        if (response == null) {
                            listener.onFailure(new BenchmarkIllegalStateException("benchmark [" + benchmarkId + "]: missing response"));
                        } else {
                            listener.onResponse(response);
                        }
                    } finally {
                        listener = null;
                    }
                }
            }
        }
    }

    private void preconditions(int num) {
        final int n = utility.executors(num).size();
        if (n < num) {
            throw new BenchmarkNodeMissingException(
                    "Insufficient executor nodes in cluster: require at least [" + num + "] found [" + n + "]");
        }
    }

    private boolean allNodesReady(final BenchmarkMetaData.Entry entry) {
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
            if (e.getValue() != BenchmarkMetaData.Entry.NodeState.READY) {
                return false;
            }
        }
        return true;
    }

    private boolean allNodesRunning(final BenchmarkMetaData.Entry entry) {
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
            if (e.getValue() != BenchmarkMetaData.Entry.NodeState.RUNNING) {
                return false;
            }
        }
        return true;
    }

    private boolean allNodesAborted(final BenchmarkMetaData.Entry entry) {
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
            if (e.getValue() != BenchmarkMetaData.Entry.NodeState.ABORTED) {
                return false;
            }
        }
        return true;
    }

    private boolean allNodesPaused(final BenchmarkMetaData.Entry entry) {
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
            if (e.getValue() != BenchmarkMetaData.Entry.NodeState.PAUSED) {
                return false;
            }
        }
        return true;
    }

    private boolean allNodesFinished(final BenchmarkMetaData.Entry entry) {
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
            if (e.getValue() == BenchmarkMetaData.Entry.NodeState.INITIALIZING ||
                e.getValue() == BenchmarkMetaData.Entry.NodeState.READY ||
                e.getValue() == BenchmarkMetaData.Entry.NodeState.RUNNING) {

                // XXX - does this cover all appropriate states? what about PAUSED?

                return false;
            }
        }
        return true;
    }

    private void log(BenchmarkMetaData.Entry entry) {
        StringBuilder sb = new StringBuilder();
        sb.append("benchmark state change: [").append(entry.benchmarkId()).append("] (").append(entry.state()).append(") [");
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> e : entry.nodeStateMap().entrySet()) {
            sb.append(" ").append(e.getKey()).append(":").append(e.getValue());
        }
        sb.append(" ]");
        logger.debug(sb.toString());
    }

    /* ** Request Handlers ** */

    /**
     * Responds to requests from the executor nodes to transmit the definition for the given benchmark.
     */
    public class BenchmarkDefinitionRequestHandler extends BaseTransportRequestHandler<BenchmarkDefinitionTransportRequest> {

        static final String ACTION = "benchmark/node/definition";

        @Override
        public BenchmarkDefinitionTransportRequest newInstance() {
            return new BenchmarkDefinitionTransportRequest();
        }

        @Override
        public void messageReceived(BenchmarkDefinitionTransportRequest request, TransportChannel channel) throws Exception {
            if (benchmarks.get(request.benchmarkId) != null) {
                final BenchmarkDefinitionActionResponse response =
                        new BenchmarkDefinitionActionResponse(benchmarks.get(request.benchmarkId).request, request.nodeId);
                channel.sendResponse(response);
            } else {
                channel.sendResponse(new BenchmarkIllegalStateException("benchmark [" + request.benchmarkId + "]: missing internal state"));
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}
