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

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.BenchmarkMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

/**
 * Mock benchmark service for testing.
 */
public class MockBenchmarkCoordinatorService extends BenchmarkCoordinatorService {

    private final Map<String, Lifecycle> lifecycles = new HashMap<>();

    @Inject
    public MockBenchmarkCoordinatorService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                           TransportService transportService, BenchmarkStateManager manager,
                                           BenchmarkUtility utility) {
        super(settings, clusterService, threadPool, transportService, manager, utility);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        super.clusterChanged(event);

        final BenchmarkMetaData meta = event.state().metaData().custom(BenchmarkMetaData.TYPE);

        if (event.metaDataChanged() && meta != null && meta.entries().size() > 0 && isOnMasterNode()) {
            observe(event);
        }
    }

    private void observe(final ClusterChangedEvent event) {

        final BenchmarkMetaData meta = event.state().metaData().custom(BenchmarkMetaData.TYPE);
        final BenchmarkMetaData prev = event.previousState().metaData().custom(BenchmarkMetaData.TYPE);

        logger.debug("--> Observing event: [{}]", event.source());

        for (final BenchmarkMetaData.Entry entry : BenchmarkMetaData.delta(prev, meta)) {

            if (lifecycles.get(entry.benchmarkId()) == null) {
                lifecycles.put(entry.benchmarkId(), new Lifecycle());
            }

            switch (entry.state()) {
                case INITIALIZING:
                    lifecycles.get(entry.benchmarkId()).didInitialize = true;
                    lifecycles.get(entry.benchmarkId()).initialMetaDataEntry = entry;
                    break;
                case RUNNING:
                    lifecycles.get(entry.benchmarkId()).didStartRunning = true;
                    break;
                case COMPLETED:
                    lifecycles.get(entry.benchmarkId()).didComplete = true;
                    break;
                case PAUSED:
                    lifecycles.get(entry.benchmarkId()).didPause = true;
                    break;
                case RESUMING:
                    lifecycles.get(entry.benchmarkId()).didResume = true;
                    break;
                case FAILED:
                    lifecycles.get(entry.benchmarkId()).didFail = true;
                    break;
                case ABORTED:
                    lifecycles.get(entry.benchmarkId()).didAbort = true;
                    break;
                default:
                    lifecycles.get(entry.benchmarkId()).didObserveUnknownState = true;
                    break;
            }
        }
    }

    public boolean isOnMasterNode() {
        final String masterNodeId = clusterService.state().nodes().getMasterNode().id();
        final String thisNodeId   = clusterService.localNode().id();
        return masterNodeId.equals(thisNodeId);
    }

    public void clearMockState() {
        lifecycles.clear();
    }

    public static class Lifecycle {
        boolean didInitialize          = false;
        boolean didStartRunning        = false;
        boolean didComplete            = false;
        boolean didPause               = false;
        boolean didResume              = false;
        boolean didFail                = false;
        boolean didAbort               = false;
        boolean didObserveUnknownState = false;

        BenchmarkMetaData.Entry initialMetaDataEntry = null;
    }

    public void validateNormalLifecycle(final String benchmarkId, final int numExecutors) {

        final Lifecycle lifecycle = lifecycles.get(benchmarkId);

        assertNotNull(lifecycle);
        assertTrue(lifecycle.didInitialize);
        assertTrue(lifecycle.didStartRunning);
        assertTrue(lifecycle.didComplete);
        assertFalse(lifecycle.didPause);
        assertFalse(lifecycle.didFail);
        assertFalse(lifecycle.didAbort);
        assertFalse(lifecycle.didObserveUnknownState);

        validateInitialState(benchmarkId, numExecutors, lifecycle.initialMetaDataEntry);
    }

    public void validatePausedLifecycle(final String benchmarkId, final int numExecutors) {

        final Lifecycle lifecycle = lifecycles.get(benchmarkId);

        assertNotNull(lifecycle);
        assertTrue(lifecycle.didInitialize);
        assertTrue(lifecycle.didStartRunning);
        assertTrue(lifecycle.didComplete);
        assertTrue(lifecycle.didPause);
        assertTrue(lifecycle.didResume);
        assertFalse(lifecycle.didFail);
        assertFalse(lifecycle.didAbort);
        assertFalse(lifecycle.didObserveUnknownState);

        validateInitialState(benchmarkId, numExecutors, lifecycle.initialMetaDataEntry);
    }

    public void validateAbortedLifecycle(final String benchmarkId, final int numExecutors) {

        final Lifecycle lifecycle = lifecycles.get(benchmarkId);

        assertNotNull(lifecycle);
        assertTrue(lifecycle.didInitialize);
        assertTrue(lifecycle.didStartRunning);
        assertTrue(lifecycle.didComplete);
        assertTrue(lifecycle.didPause);
        assertTrue(lifecycle.didResume);
        assertFalse(lifecycle.didFail);
        assertTrue(lifecycle.didAbort);
        assertFalse(lifecycle.didObserveUnknownState);

        validateInitialState(benchmarkId, numExecutors, lifecycle.initialMetaDataEntry);
    }

    private void validateInitialState(final String benchmarkId, final int numExecutors, final BenchmarkMetaData.Entry entry) {

        assertNotNull(entry);
        assertThat(entry.benchmarkId(), equalTo(benchmarkId));
        assertThat(entry.nodeStateMap().size(), equalTo(numExecutors));
        for (final BenchmarkMetaData.Entry.NodeState ns : entry.nodeStateMap().values()) {
            assertThat(ns, equalTo(BenchmarkMetaData.Entry.NodeState.READY));
        }
    }
}
