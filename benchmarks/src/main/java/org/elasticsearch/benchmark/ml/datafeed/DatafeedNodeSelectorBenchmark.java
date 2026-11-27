/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.ml.datafeed;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.benchmark.routing.allocation.Allocators;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.ml.datafeed.DatafeedNodeSelector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Micro-benchmark for {@link DatafeedNodeSelector#selectNode}, the method the master runs for every
 * unassigned datafeed on every cluster-state update. It models a cluster resembling SDH-10462: a large
 * total index count with one heavy data stream of many backing indices that the datafeed reads from.
 *
 * <p>The {@code jobState} parameter switches between the two code paths that matter:
 * <ul>
 *   <li>{@code opened} — the job is open and non-stale, so the datafeed is assignable and the selector
 *       resolves the datafeed's index patterns against the cluster state (the expensive path);</li>
 *   <li>{@code stale} / {@code closed} — the job is not assignable, so the selector short-circuits before
 *       that index-pattern resolution.</li>
 * </ul>
 * Comparing {@code opened} against {@code stale}/{@code closed} at a given {@code backingIndices} count
 * measures the per-datafeed cost removed by the short-circuit during mass reassignment.
 *
 * <p>Note: this benchmark deliberately builds no routing table. The expensive, fix-relevant work is the
 * index-pattern resolution ({@code IndexNameExpressionResolver#concreteIndexNames}) performed before the
 * routing is inspected, so an absent routing table does not change which code path each {@code jobState}
 * exercises. It also avoids {@code IndexShardRoutingTable}'s dependency on randomized-test infrastructure,
 * which is unavailable on a JMH worker thread.
 */
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class DatafeedNodeSelectorBenchmark {

    static {
        // Install the logging SPI provider before any ES class with a static logger is loaded,
        // otherwise DatafeedNodeSelector (and friends) fail to initialize under JMH.
        BenchmarkLogging.configure();
    }

    private static final String HEAVY_STREAM = "heavy-stream";
    private static final String JOB_ID = "job";
    private static final String DATAFEED_ID = "datafeed";
    private static final int TOTAL_INDICES = 14000;

    @Param({ "100", "500", "1000" })
    public int backingIndices;

    @Param({ "opened", "stale", "closed" })
    public String jobState;

    private ClusterState clusterState;
    private IndexNameExpressionResolver resolver;
    private List<DiscoveryNode> candidateNodes;
    private List<String> datafeedIndices;

    @Setup
    public void setUp() {
        DiscoveryNode node = Allocators.newNode("node_0", Map.of());
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node).build();
        candidateNodes = List.of(node, Allocators.newNode("other_node", Map.of()));
        datafeedIndices = List.of(HEAVY_STREAM);

        resolver = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            new SystemIndices(List.of()),
            DefaultProjectResolver.INSTANCE
        );

        Metadata.Builder metadata = new Metadata.Builder().putCustom(MlMetadata.TYPE, new MlMetadata.Builder().build())
            .putCustom(PersistentTasksCustomMetadata.TYPE, buildTasks());

        // The heavy data stream the datafeed reads from.
        List<Index> backing = new ArrayList<>(backingIndices);
        for (int gen = 1; gen <= backingIndices; gen++) {
            IndexMetadata indexMetadata = indexMetadata(DataStream.getDefaultBackingIndexName(HEAVY_STREAM, gen));
            metadata.put(indexMetadata, false);
            backing.add(indexMetadata.getIndex());
        }
        metadata.put(DataStream.builder(HEAVY_STREAM, backing).build());

        // Fill the rest of the cluster with ordinary indices to approximate total cluster size.
        for (int i = backing.size(); i < TOTAL_INDICES; i++) {
            metadata.put(indexMetadata("idx-" + i), false);
        }

        clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).nodes(nodes).build();
    }

    private PersistentTasksCustomMetadata buildTasks() {
        PersistentTasksCustomMetadata.Builder builder = PersistentTasksCustomMetadata.builder();
        builder.addTask(
            MlTasks.jobTaskId(JOB_ID),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams(JOB_ID),
            new PersistentTasksCustomMetadata.Assignment("node_0", "test assignment")
        );
        JobState state = jobState.equals("closed") ? JobState.CLOSED : JobState.OPENED;
        boolean stale = jobState.equals("stale");
        builder.updateTaskState(
            MlTasks.jobTaskId(JOB_ID),
            new JobTaskState(state, builder.getLastAllocationId() - (stale ? 1 : 0), null, Instant.now())
        );
        return builder.build();
    }

    private static IndexMetadata indexMetadata(String name) {
        return IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    @Benchmark
    public Object selectNode() {
        return new DatafeedNodeSelector(clusterState, resolver, DATAFEED_ID, JOB_ID, datafeedIndices, SearchRequest.DEFAULT_INDICES_OPTIONS)
            .selectNode(candidateNodes);
    }
}
