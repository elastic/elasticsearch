/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.translog;

import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.engine.TranslogOperationAsserter;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.xcontent.XContentType;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Measures the batch translog record ({@link IndexOperationBatch.TranslogRecord}) on both sides of the
 * translog, for one batch of {@code docCount} rows whose per-row outcome is fixed by {@code scenario}:
 * <ul>
 * <li>{@code indexBatchTranslogWrite}: the write {@code InternalEngine#indexBatch} issues once the rows
 * are in Lucene, {@code translog.add(subBatch.toTranslogRecord(rowStatuses, noOpReasons))}, against a
 * real {@link Translog}. Lucene indexing itself is excluded.</li>
 * <li>{@code explode}: the recovery read, decoding the record into one operation per replayable row.</li>
 * <li>{@code getIndexOp}: the realtime-get read of a single indexed row. It targets the last indexed row,
 * the worst case for the sparse row lookups, and is not defined for {@code all_noop}, which has none.</li>
 * </ul>
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TranslogBatchBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    public static final String ALL_SUCCESS = "all_success";
    public static final String NOOP_10PCT = "noop_10pct";
    public static final String PREFLIGHT_1PCT = "preflight_1pct";
    public static final String ALL_NOOP = "all_noop";

    /** The engine's inputs to the translog write for one batch, and the record they produce. */
    private record Batch(EscfBatch escfBatch, IndexOperationBatch batch, byte[] rowStatuses, String[] noOpReasons, int lastIndexedRow) {

        IndexOperationBatch.TranslogRecord toTranslogRecord() {
            return batch.toTranslogRecord(rowStatuses, noOpReasons);
        }
    }

    /**
     * Mirrors {@code processSubBatch}: one status byte per row, a reason for every no-op row, seqNos
     * handed out contiguously to the rows that reached Lucene, preflight failures left UNASSIGNED.
     * Failed rows are spread evenly over the batch.
     */
    private static Batch buildBatch(int docCount, String scenario) throws IOException {
        final BulkItemRequest[] items = new BulkItemRequest[docCount];
        final List<BytesReference> sources = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            final BytesReference source = new BytesArray("{\"field_a\":\"value-" + i + "\",\"field_b\":" + i + ",\"field_c\":true}");
            sources.add(source);
            items[i] = new BulkItemRequest(i, new IndexRequest("index").id("doc-" + i).source(source, XContentType.JSON));
        }
        final EscfBatch escfBatch = EscfEncoder.encode(sources, XContentType.JSON);
        final IndexOperationBatch batch = IndexOperationBatch.initFromBulk(
            items,
            0,
            docCount,
            escfBatch,
            Engine.Operation.Origin.PRIMARY,
            1L,
            0L
        );
        final byte[] rowStatuses = new byte[docCount];
        String[] noOpReasons = null;
        int lastIndexedRow = -1;
        long seqNo = 0;
        for (int i = 0; i < docCount; i++) {
            final byte status = switch (scenario) {
                case ALL_SUCCESS -> IndexOperationBatch.TranslogRecord.ROW_INDEXED;
                case NOOP_10PCT -> i % 10 == 0
                    ? IndexOperationBatch.TranslogRecord.ROW_NO_OP
                    : IndexOperationBatch.TranslogRecord.ROW_INDEXED;
                case PREFLIGHT_1PCT -> i % 100 == 0
                    ? IndexOperationBatch.TranslogRecord.ROW_PREFLIGHT_ERROR
                    : IndexOperationBatch.TranslogRecord.ROW_INDEXED;
                case ALL_NOOP -> IndexOperationBatch.TranslogRecord.ROW_NO_OP;
                default -> throw new IllegalArgumentException("unknown scenario [" + scenario + "]");
            };
            rowStatuses[i] = status;
            switch (status) {
                case IndexOperationBatch.TranslogRecord.ROW_INDEXED -> {
                    ByteUtils.writeLongLE(seqNo++, batch.seqNoBytes().bytes, i * 8);
                    ByteUtils.writeLongLE(1L, batch.versionBytes().bytes, i * 8);
                    lastIndexedRow = i;
                }
                case IndexOperationBatch.TranslogRecord.ROW_NO_OP -> {
                    if (noOpReasons == null) {
                        noOpReasons = new String[docCount];
                    }
                    noOpReasons[i] = "failure on row " + i;
                    ByteUtils.writeLongLE(seqNo++, batch.seqNoBytes().bytes, i * 8);
                }
                case IndexOperationBatch.TranslogRecord.ROW_PREFLIGHT_ERROR -> ByteUtils.writeLongLE(
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    batch.seqNoBytes().bytes,
                    i * 8
                );
                default -> throw new AssertionError("unknown row status [" + status + "]");
            }
        }
        return new Batch(escfBatch, batch, rowStatuses, noOpReasons, lastIndexedRow);
    }

    /** The batch for the write and explode benchmarks, plus a translog to write into. */
    @State(Scope.Benchmark)
    public static class BatchState {

        @Param({ "50000" })
        public int docCount;

        @Param({ ALL_SUCCESS, NOOP_10PCT, PREFLIGHT_1PCT, ALL_NOOP })
        public String scenario;

        private final ShardId shardId = new ShardId("index", "_na_", 0);

        Batch batch;
        IndexOperationBatch.TranslogRecord record;
        private Path translogDir;
        Translog translog;

        @Setup(Level.Trial)
        public void setUp() throws IOException {
            batch = buildBatch(docCount, scenario);
            record = batch.toTranslogRecord();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            batch.escfBatch().close();
        }

        /** A fresh translog per iteration bounds the on-disk growth; nothing is fsynced, as in the engine's add path. */
        @Setup(Level.Iteration)
        public void openTranslog() throws IOException {
            translogDir = Files.createTempDirectory("translog-batch-benchmark");
            final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
            final IndexMetadata indexMetadata = IndexMetadata.builder(shardId.getIndexName())
                .settings(settings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            final TranslogConfig config = new TranslogConfig(
                shardId,
                translogDir,
                new IndexSettings(indexMetadata, Settings.EMPTY),
                BigArrays.NON_RECYCLING_INSTANCE
            );
            final String translogUUID = Translog.createEmptyTranslog(translogDir, SequenceNumbers.NO_OPS_PERFORMED, shardId, 1L);
            translog = new Translog(
                config,
                translogUUID,
                new TranslogDeletionPolicy(),
                () -> SequenceNumbers.NO_OPS_PERFORMED,
                () -> 1L,
                seqNos -> {},
                TranslogOperationAsserter.DEFAULT
            );
        }

        @TearDown(Level.Iteration)
        public void closeTranslog() throws IOException {
            translog.close();
            IOUtils.rm(translogDir);
        }
    }

    /** The same batch for the single-row read, limited to the scenarios that have an indexed row to fetch. */
    @State(Scope.Benchmark)
    public static class IndexedRowState {

        @Param({ "50000" })
        public int docCount;

        @Param({ ALL_SUCCESS, NOOP_10PCT, PREFLIGHT_1PCT })
        public String scenario;

        private Batch batch;
        IndexOperationBatch.TranslogRecord record;
        int lastIndexedRow;

        @Setup(Level.Trial)
        public void setUp() throws IOException {
            batch = buildBatch(docCount, scenario);
            record = batch.toTranslogRecord();
            lastIndexedRow = batch.lastIndexedRow();
            if (lastIndexedRow < 0) {
                throw new IllegalStateException("scenario [" + scenario + "] has no indexed row to fetch");
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            batch.escfBatch().close();
        }
    }

    @Benchmark
    public Translog.Location indexBatchTranslogWrite(BatchState state) throws IOException {
        return state.translog.add(state.batch.toTranslogRecord());
    }

    @Benchmark
    public List<Translog.Operation> explode(BatchState state) throws IOException {
        return state.record.explode();
    }

    @Benchmark
    public Translog.Index getIndexOp(IndexedRowState state) throws IOException {
        return state.record.getIndexOp(state.lastIndexedRow);
    }
}
