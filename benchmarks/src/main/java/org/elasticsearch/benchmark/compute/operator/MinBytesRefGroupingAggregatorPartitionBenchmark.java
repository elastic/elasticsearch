/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.MinBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the partition split+merge cycle for {@link org.elasticsearch.compute.aggregation.MinBytesRefGroupingAggregatorFunction}.
 * <p>
 * The {@code groups * valueLength} product determines which storage layout is used:
 * below 400 MB total the dense flat-buffer path is taken; above that the sparse
 * per-{@code BytesRef} path is used.  With the default params, {@code groups=1_000_000,
 * valueLength=512} (512 MB) exercises the sparse path; all other combinations exercise
 * the dense path.
 */
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 2, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector", "-Xms8g", "-Xmx8g" })
public class MinBytesRefGroupingAggregatorPartitionBenchmark {
    static {
        BenchmarkLogging.configure();
    }

    private static final int NUM_PARTITIONS = PartitionedHashTable.NUM_PARTITIONS;
    private static final int PARTITION_WRITE_BATCH = PartitionedHashTable.PARTITION_WRITE_BATCH;
    private static final int BATCH_TOTAL = NUM_PARTITIONS * PARTITION_WRITE_BATCH;
    private static final int ROWS_PER_PAGE = 4096;

    @Param({ "10000", "100000", "1000000" })
    int groups;

    @Param({ "8", "32", "128", "512" })
    int valueLength;

    @Param({ "false", "true" })
    boolean variableLength;

    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private LocalCircuitBreaker localBreaker;
    private DriverContext driverContext;
    private NoopCircuitBreaker partitionBreaker;
    private MinBytesRefAggregatorFunctionSupplier supplier;
    private GroupingAggregatorFunction srcAgg;

    private int numBatches;
    private int[] firstIds;
    private short[][] shiftedIds;
    private int[] batchSizes;
    private int[][] batchPartitionCounts;
    private int[][] partitionOffsets;
    private int[] partitionCount;
    private int[][] dstIds;

    @Setup(Level.Trial)
    public void setup() {
        var breakerService = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.EMPTY,
            List.of(),
            clusterSettings
        );
        var recycler = new PageCacheRecycler(Settings.EMPTY);
        var bigArrays = new BigArrays(recycler, breakerService, "request");
        var breaker = breakerService.getBreaker("request");
        localBreaker = new LocalCircuitBreaker(
            breaker,
            BlockFactory.LOCAL_BREAKER_OVER_RESERVED_DEFAULT_SIZE.getBytes(),
            BlockFactory.LOCAL_BREAKER_OVER_RESERVED_DEFAULT_MAX_SIZE.getBytes()
        );
        var blockFactory = BlockFactory.builder(bigArrays).breaker(breaker).build().newChildFactory(localBreaker);
        driverContext = new DriverContext(bigArrays, blockFactory, null);
        partitionBreaker = new NoopCircuitBreaker("partition");
        supplier = new MinBytesRefAggregatorFunctionSupplier();

        srcAgg = supplier.groupingAggregator(driverContext, List.of(0));
        var rng = new SplittableRandom(42);
        for (int start = 0; start < groups; start += ROWS_PER_PAGE) {
            feedPage(start, Math.min(ROWS_PER_PAGE, groups - start), rng);
        }

        numBatches = Math.ceilDiv(groups, BATCH_TOTAL);
        firstIds = new int[numBatches];
        shiftedIds = new short[numBatches][];
        batchSizes = new int[numBatches];
        batchPartitionCounts = new int[numBatches][];
        partitionOffsets = new int[numBatches][];
        partitionCount = new int[NUM_PARTITIONS];

        int[] cumulativeCounts = new int[NUM_PARTITIONS];
        for (int b = 0; b < numBatches; b++) {
            firstIds[b] = b * BATCH_TOTAL;
            int inBatch = Math.min(BATCH_TOTAL, groups - b * BATCH_TOTAL);
            batchSizes[b] = inBatch;
            batchPartitionCounts[b] = new int[NUM_PARTITIONS];
            partitionOffsets[b] = cumulativeCounts.clone();
            shiftedIds[b] = new short[NUM_PARTITIONS * PARTITION_WRITE_BATCH];
            for (int i = 0; i < inBatch; i++) {
                int p = i % NUM_PARTITIONS;
                int pos = batchPartitionCounts[b][p];
                shiftedIds[b][p * PARTITION_WRITE_BATCH + pos] = (short) i;
                batchPartitionCounts[b][p]++;
            }
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                cumulativeCounts[p] += batchPartitionCounts[b][p];
            }
        }
        System.arraycopy(cumulativeCounts, 0, partitionCount, 0, NUM_PARTITIONS);

        dstIds = new int[NUM_PARTITIONS][1];
        int offset = 0;
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            dstIds[p][0] = offset;
            offset += partitionCount[p];
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        srcAgg.close();
        localBreaker.close();
    }

    @Benchmark
    public void splitOnly() {
        var splitter = srcAgg.createPartitioningSplitter(partitionBreaker);
        for (int b = 0; b < numBatches; b++) {
            splitter.split(firstIds[b], shiftedIds[b], batchSizes[b], batchPartitionCounts[b], partitionOffsets[b]);
        }
        var state = splitter.finish();
        state.releaseAll(partitionBreaker);
    }

    @Benchmark
    public void splitAndMerge() {
        var splitter = srcAgg.createPartitioningSplitter(partitionBreaker);
        for (int b = 0; b < numBatches; b++) {
            splitter.split(firstIds[b], shiftedIds[b], batchSizes[b], batchPartitionCounts[b], partitionOffsets[b]);
        }
        var state = splitter.finish();
        try (var dst = supplier.groupingAggregator(driverContext, List.of(0))) {
            dst.maybeEnsureCapacity(groups);
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                if (partitionCount[p] > 0) {
                    dst.combinePartition(state, p, true, dstIds[p], partitionCount[p]);
                }
            }
        }
        state.releaseAll(partitionBreaker);
    }

    private void feedPage(int groupStart, int count, SplittableRandom rng) {
        int minLen = valueLength / 2;
        int maxLen = valueLength + valueLength / 2;
        byte[] buf = new byte[maxLen];
        BytesRef bytesRef = new BytesRef(buf);
        var valuesBuilder = driverContext.blockFactory().newBytesRefVectorBuilder(count);
        var groupsBuilder = driverContext.blockFactory().newIntVectorFixedBuilder(count);
        for (int i = 0; i < count; i++) {
            int len = variableLength ? rng.nextInt(minLen, maxLen + 1) : valueLength;
            rng.nextBytes(buf);
            bytesRef.length = len;
            valuesBuilder.appendBytesRef(bytesRef);
            groupsBuilder.appendInt(i, groupStart + i);
        }
        // valuesVector is transferred to the page via asBlock(); page.releaseBlocks() frees it.
        // groupsVector is not in the page so must be closed separately.
        var groupsVector = groupsBuilder.build();
        Page page = new Page(valuesBuilder.build().asBlock());
        try (var addInput = srcAgg.prepareProcessRawInputPage(null, page)) {
            addInput.add(0, groupsVector);
        }
        page.releaseBlocks();
        groupsVector.close();
    }
}
