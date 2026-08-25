/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.flattened;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.lucene.read.DelegatingBlockLoaderFactory;
import org.elasticsearch.index.codec.flattened.ColumnarKeyedBinaryDocValues;
import org.elasticsearch.index.fielddata.KeyLookupArrayOrderBinaryDocValues;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.benchmark.index.codec.flattened.FlattenedKeyedIngestBenchmark.KEYED_FIELD;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.MAX_DOCS_PER_BLOCK_DEFAULT;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.TARGET_BLOCK_BYTES_DEFAULT;

/**
 * Read cost for the flattened {@code ._keyed} column over a single force-merged segment, comparing
 * the {@code ROW} and {@code COLUMNAR} layouts on three access patterns:
 *
 * <ul>
 *   <li>{@code fullBlob} — iterates every doc and consumes its complete binary blob via
 *       {@link BinaryDocValues#binaryValue()}. For COLUMNAR this exercises the block transpose that
 *       rebuilds a row-layout blob from the per-key columnar runs — the overhead that synthetic
 *       source reconstruction pays.</li>
 *   <li>{@code singleKey} — for every doc extracts the value of exactly one sub-field (key {@code "key0"}).
 *       ROW must decompress and scan the full per-doc blob; COLUMNAR decompresses only that key's
 *       run, directly via
 *       {@link ColumnarKeyedBinaryDocValues#advanceExactKey(int)} +
 *       {@link ColumnarKeyedBinaryDocValues#nextKeyValue()}.</li>
 *   <li>{@code singleKeyLinear} — the same single-key extraction but always via the linear blob scan,
 *       regardless of layout. Comparing this to {@code singleKey} on COLUMNAR isolates the speedup
 *       from the key-lookup fast path.</li>
 *   <li>{@code sparseSingleKey} — extracts one key for a random stride of docs controlled by
 *       {@code selectivityPercent}. At {@code selectivityPercent=1} each touched doc is likely in a
 *       different block, so wasted decompression per block is fully measured. This is the access
 *       pattern where larger block sizes can regress vs the default 8 KiB.</li>
 *   <li>{@code blockLoaderPerDoc} — {@link BlockLoader.OptionalColumnAtATimeReader} baseline.
 *       Replicates {@code BinaryKeyedBlockDocValuesReader.read} without the batch reader: one
 *       {@link KeyLookupArrayOrderBinaryDocValues#advanceExact} + {@code docValueCount} +
 *       {@code nextValue} per document, building a real {@link BlockLoader.Block} per page.
 *       Use this to isolate the per-doc seek overhead.</li>
 *   <li>{@code blockLoaderBatch} — new batch path.
 *       {@link ColumnarKeyedBinaryDocValues#keyColumnReader(int)} produces a forward-only column
 *       cursor; one {@link BlockLoader.OptionalColumnAtATimeReader#tryRead} call per page replaces
 *       the per-doc binary searches and decompression resets that {@code blockLoaderPerDoc} pays.</li>
 * </ul>
 *
 * <p>The primary decision metrics are:
 * <ul>
 *   <li>COLUMNAR vs ROW delta on {@code singleKey} at high key cardinality — the more sub-fields
 *       a document carries, the larger the ratio of wasted decompression in the row path.</li>
 *   <li>{@code sparseSingleKey} at {@code selectivityPercent=1} while sweeping {@code targetBlockBytes}
 *       — the veto: a large regression here argues against large blocks for selective workloads.</li>
 *   <li>{@code blockLoaderBatch} vs {@code blockLoaderPerDoc} at {@code selectivityPercent=100} —
 *       the batch speedup; the gap should widen from {@code FEW_KEYS} to {@code MANY_KEYS}.</li>
 *   <li>{@code blockLoaderBatch} vs {@code blockLoaderPerDoc} at {@code selectivityPercent=1} —
 *       parity guard: if {@code blockLoaderBatch} regresses here, {@code advance()} is decompressing
 *       blocks it should skip.</li>
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class FlattenedKeyedReadBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /** Number of documents per {@link BlockLoader} page (matches the compute engine default). */
    private static final int BLOCK_LENGTH = 1024;

    /**
     * Compute-layer {@link BlockFactory} backed by a no-op circuit breaker; shared across all
     * benchmark invocations so that block allocation does not dominate per-invocation timing.
     */
    private static final BlockFactory COMPUTE_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    /**
     * {@link BlockLoader.BlockFactory} wrapper for the block-loader arms, delegating to
     * {@link #COMPUTE_BLOCK_FACTORY}.
     */
    private static final BlockLoader.BlockFactory LOADER_FACTORY = new DelegatingBlockLoaderFactory(COMPUTE_BLOCK_FACTORY) {
        @Override
        public BlockLoader.Block constantNulls(int count) {
            return factory.newConstantNullBlock(count);
        }
    };

    @Param({ "ROW", "COLUMNAR" })
    private FlattenedKeyedIngestBenchmark.Layout layout;

    @Param({ "FEW_KEYS", "MEDIUM_KEYS", "MANY_KEYS" })
    private String workload;

    @Param("200000")
    private int numDocs;

    /** Target uncompressed bytes per block (COLUMNAR only). Default matches the production default. */
    @Param("" + TARGET_BLOCK_BYTES_DEFAULT)
    private int targetBlockBytes;

    /** Maximum documents per block (COLUMNAR only). Default matches the production default. */
    @Param("" + MAX_DOCS_PER_BLOCK_DEFAULT)
    private int maxDocsPerBlock;

    /**
     * Percentage of documents touched by {@link #sparseSingleKey}. {@code 100} means every doc
     * (same density as the sequential arms); {@code 1} means roughly one doc per block at the
     * default 8 KiB / 1024-doc thresholds, so each hit lands in a fresh block.
     */
    @Param("100")
    private int selectivityPercent;

    private Directory directory;
    private DirectoryReader reader;

    /**
     * The single force-merged leaf; cached at {@link Level#Trial} to avoid repeated
     * {@code reader.leaves()} allocation inside the block-loader benchmark methods.
     */
    private LeafReaderContext leaf;

    /**
     * Pre-allocated doc-ID buffer for block-loader page iteration. Populated in-place per page so
     * that no {@code int[]} allocation occurs inside the benchmark hot loop.
     */
    private int[] pageDocIds;

    /**
     * {@code "key0\0"} — the null-byte terminator ensures this prefix does not accidentally match
     * {@code "key00"}, {@code "key01"}, etc., during the linear blob scan.
     */
    private byte[] keyWithSep;

    /**
     * Key ordinal for {@code "key0"} in the segment dictionary; {@code -1} if absent (should not
     * occur with the deterministic data generator). Only valid for the COLUMNAR arm.
     */
    private int keyOrdinal = -1;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        directory = FSDirectory.open(Files.createTempDirectory("flattened-keyed-read-"));
        List<List<BytesRef>> data = FlattenedKeyedData.generate(workload, numDocs);
        try (
            IndexWriter writer = new IndexWriter(
                directory,
                new IndexWriterConfig().setCodec(layout.codec(targetBlockBytes, maxDocsPerBlock))
            )
        ) {
            for (int i = 0; i < numDocs; i++) {
                final LuceneDocument doc = new LuceneDocument();
                for (BytesRef kv : data.get(i)) {
                    KeyedArrayOrderInlineNull.recordValue(doc, KEYED_FIELD, kv);
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);

        // Build the search target: "key0" + separator byte.
        byte[] keyBytes = FlattenedKeyedData.keyBytes(0);
        keyWithSep = new byte[keyBytes.length + 1];
        System.arraycopy(keyBytes, 0, keyWithSep, 0, keyBytes.length);
        keyWithSep[keyBytes.length] = 0;

        // For COLUMNAR arm: resolve the key ordinal once at setup so the benchmark loop is tight.
        if (layout == FlattenedKeyedIngestBenchmark.Layout.COLUMNAR) {
            for (LeafReaderContext lrc : reader.leaves()) {
                BinaryDocValues bdv = lrc.reader().getBinaryDocValues(KEYED_FIELD);
                if (bdv instanceof ColumnarKeyedBinaryDocValues columnar) {
                    keyOrdinal = columnar.lookupKeyOrdinal(new BytesRef(keyBytes));
                    break;
                }
            }
        }

        // Cache the single force-merged leaf and pre-allocate the page buffer for block-loader arms.
        leaf = reader.leaves().isEmpty() ? null : reader.leaves().get(0);
        pageDocIds = new int[BLOCK_LENGTH];
    }

    /**
     * Iterates every document and consumes its raw binary blob. Exercises block decompression and,
     * for COLUMNAR, the full transpose from per-key columnar runs back to a row-layout blob.
     */
    @Benchmark
    public void fullBlob(Blackhole bh) throws IOException {
        for (LeafReaderContext leaf : reader.leaves()) {
            final BinaryDocValues bdv = leaf.reader().getBinaryDocValues(KEYED_FIELD);
            if (bdv == null) continue;
            while (bdv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                bh.consume(bdv.binaryValue());
            }
        }
    }

    /**
     * For COLUMNAR: extracts values for {@code "key0"} via the columnar fast path — only the target
     * key's compressed run is decompressed per block. For ROW: decompresses the full per-doc blob
     * and scans it linearly for the target key.
     */
    @Benchmark
    public void singleKey(Blackhole bh) throws IOException {
        for (LeafReaderContext leaf : reader.leaves()) {
            final BinaryDocValues bdv = leaf.reader().getBinaryDocValues(KEYED_FIELD);
            if (bdv == null) continue;
            if (bdv instanceof ColumnarKeyedBinaryDocValues columnar && keyOrdinal >= 0) {
                singleKeyColumnar(bh, columnar);
            } else {
                singleKeyLinearScan(bh, bdv);
            }
        }
    }

    /**
     * Always uses the linear blob scan, regardless of layout. Comparing {@code singleKey} on
     * COLUMNAR against this benchmark isolates the speedup from the key-lookup fast path.
     */
    @Benchmark
    public void singleKeyLinear(Blackhole bh) throws IOException {
        for (LeafReaderContext leaf : reader.leaves()) {
            final BinaryDocValues bdv = leaf.reader().getBinaryDocValues(KEYED_FIELD);
            if (bdv == null) continue;
            singleKeyLinearScan(bh, bdv);
        }
    }

    /**
     * Extracts one key for a strided subset of documents controlled by {@link #selectivityPercent}.
     * At {@code selectivityPercent=1} consecutive touched docs are ~100 docs apart, meaning each hit
     * is likely in a different block. This is the access pattern where larger blocks pay a
     * decompression tax: 128 KiB must be decompressed per hit instead of 8 KiB.
     *
     * <p>Sweep {@code -p targetBlockBytes=8192,32768,131072 -p selectivityPercent=1} to find the
     * crossover point between compression-ratio win and per-hit decompression cost.
     */
    @Benchmark
    public void sparseSingleKey(Blackhole bh) throws IOException {
        final int step = Math.max(1, 100 / selectivityPercent);
        for (LeafReaderContext leaf : reader.leaves()) {
            final BinaryDocValues bdv = leaf.reader().getBinaryDocValues(KEYED_FIELD);
            if (bdv == null) continue;
            final int maxDoc = leaf.reader().maxDoc();
            if (bdv instanceof ColumnarKeyedBinaryDocValues columnar && keyOrdinal >= 0) {
                for (int doc = 0; doc < maxDoc; doc += step) {
                    if (columnar.advanceExact(doc) == false) continue;
                    int slots = columnar.advanceExactKey(keyOrdinal);
                    for (int s = 0; s < slots; s++) {
                        bh.consume(columnar.nextKeyValue());
                    }
                }
            } else {
                for (int doc = 0; doc < maxDoc; doc += step) {
                    if (bdv.advanceExact(doc) == false) continue;
                    bh.consume(findKeyInBlob(bdv.binaryValue(), keyWithSep));
                }
            }
        }
    }

    /**
     * Fast path: decompresses only the target key's columnar run per block.
     * {@code advanceExactKey(ordinal)} returns the slot count for that key in the current doc;
     * zero means the key is absent (the caller pays only directory-lookup cost, no decompression).
     */
    private void singleKeyColumnar(Blackhole bh, ColumnarKeyedBinaryDocValues columnar) throws IOException {
        while (columnar.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            columnar.advanceExact(columnar.docID());
            int slotCount = columnar.advanceExactKey(keyOrdinal);
            for (int s = 0; s < slotCount; s++) {
                bh.consume(columnar.nextKeyValue());
            }
        }
    }

    /**
     * Linear scan: decompresses the full per-doc blob and searches for the target key prefix using
     * proper {@link KeyedArrayOrderInlineNull} slot framing.
     */
    private void singleKeyLinearScan(Blackhole bh, BinaryDocValues bdv) throws IOException {
        while (bdv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            bh.consume(findKeyInBlob(bdv.binaryValue(), keyWithSep));
        }
    }

    /**
     * Scans a {@link KeyedArrayOrderInlineNull}-encoded blob for a slot whose key matches
     * {@code keyWithSep} (= {@code key + 0x00}). Returns {@code true} and stops at the first match.
     *
     * <p>Blob encoding: {@code [(valueLen+1) vint][key\0value]...} per slot. A prefix of {@code 0}
     * marks a null slot (no value bytes follow the key).
     */
    static boolean findKeyInBlob(BytesRef blob, byte[] keyWithSep) {
        byte[] bytes = blob.bytes;
        int pos = blob.offset;
        int end = blob.offset + blob.length;

        while (pos < end) {
            // Read valueLen+1 as a vint.
            int v = 0, shift = 0;
            byte b;
            do {
                b = bytes[pos++];
                v |= (b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);

            // Compare the key prefix against keyWithSep (= key bytes + separator \0).
            if (pos + keyWithSep.length <= end) {
                boolean match = true;
                for (int i = 0; i < keyWithSep.length; i++) {
                    if (bytes[pos + i] != keyWithSep[i]) {
                        match = false;
                        break;
                    }
                }
                if (match) return true;
            }

            // Advance past this slot: scan forward to find the separator, then skip the value.
            while (pos < end && bytes[pos++] != 0) {
                /* skip key bytes + separator */ }
            pos += v == 0 ? 0 : v - 1; // skip value bytes (null slot: v == 0, so 0 value bytes)
        }
        return false;
    }

    /**
     * Baseline for the block-loader comparison. For each page of {@link #BLOCK_LENGTH} documents,
     * reads one sub-field via the per-doc {@link KeyLookupArrayOrderBinaryDocValues} path —
     * one {@code advanceExact} + column seek per document — and builds a real
     * {@link BlockLoader.Block}. This replicates what {@code BinaryKeyedBlockDocValuesReader.read}
     * does without the batch reader, so the delta against {@link #blockLoaderBatch} isolates the
     * cost of per-doc binary searches and decompression resets.
     *
     * <p>Only meaningful for the {@code COLUMNAR} layout. Returns immediately for {@code ROW}.
     */
    @Benchmark
    public void blockLoaderPerDoc(Blackhole bh) throws IOException {
        if (leaf == null || layout != FlattenedKeyedIngestBenchmark.Layout.COLUMNAR || keyOrdinal < 0) return;
        final BinaryDocValues bdv = leaf.reader().getBinaryDocValues(KEYED_FIELD);
        if (!(bdv instanceof ColumnarKeyedBinaryDocValues columnar)) return;
        final KeyLookupArrayOrderBinaryDocValues filtered = new KeyLookupArrayOrderBinaryDocValues(
            columnar,
            new BytesRef(FlattenedKeyedData.keyBytes(0))
        );
        final int maxDoc = leaf.reader().maxDoc();
        for (int base = 0; base < maxDoc; base += BLOCK_LENGTH) {
            final int count = Math.min(BLOCK_LENGTH, maxDoc - base);
            for (int i = 0; i < count; i++) {
                pageDocIds[i] = base + i;
            }
            final BlockLoader.Block block;
            try (BlockLoader.BytesRefBuilder builder = LOADER_FACTORY.bytesRefs(count)) {
                for (int i = 0; i < count; i++) {
                    if (filtered.advanceExact(pageDocIds[i]) == false) {
                        builder.appendNull();
                    } else {
                        int n = filtered.docValueCount();
                        if (n == 1) {
                            builder.appendBytesRef(filtered.nextValue());
                        } else {
                            builder.beginPositionEntry();
                            for (int v = 0; v < n; v++) {
                                builder.appendBytesRef(filtered.nextValue());
                            }
                            builder.endPositionEntry();
                        }
                    }
                }
                block = builder.build();
            }
            bh.consume(block);
            block.close();
        }
    }

    /**
     * New batch path for the block-loader comparison. For each page of {@link #BLOCK_LENGTH}
     * documents, loads one sub-field via {@link ColumnarKeyedBinaryDocValues#keyColumnReader(int)}
     * — one forward scan per column block instead of one seek per document. Compare against
     * {@link #blockLoaderPerDoc} to quantify the benefit of the batch path.
     *
     * <p>Only meaningful for the {@code COLUMNAR} layout. Returns immediately for {@code ROW}.
     */
    @Benchmark
    public void blockLoaderBatch(Blackhole bh) throws IOException {
        if (leaf == null || layout != FlattenedKeyedIngestBenchmark.Layout.COLUMNAR || keyOrdinal < 0) return;
        final BinaryDocValues bdv = leaf.reader().getBinaryDocValues(KEYED_FIELD);
        if (!(bdv instanceof ColumnarKeyedBinaryDocValues columnar)) return;
        final BlockLoader.OptionalColumnAtATimeReader batchReader = columnar.keyColumnReader(keyOrdinal);
        if (batchReader == null) return;
        final int maxDoc = leaf.reader().maxDoc();
        for (int base = 0; base < maxDoc; base += BLOCK_LENGTH) {
            final int count = Math.min(BLOCK_LENGTH, maxDoc - base);
            for (int i = 0; i < count; i++) {
                pageDocIds[i] = base + i;
            }
            final BlockLoader.Block block = batchReader.tryRead(
                LOADER_FACTORY,
                new PageDocs(pageDocIds, count),
                0,
                false,
                null,
                false,
                false
            );
            bh.consume(block);
            if (block != null) block.close();
        }
    }

    /**
     * Minimal {@link BlockLoader.Docs} over a pre-allocated int array with an explicit count.
     * Used by the block-loader benchmark arms to avoid allocating a new array per page.
     */
    private static final class PageDocs implements BlockLoader.Docs {
        private final int[] docs;
        private final int count;

        PageDocs(int[] docs, int count) {
            this.docs = docs;
            this.count = count;
        }

        @Override
        public int count() {
            return count;
        }

        @Override
        public int get(int i) {
            return docs[i];
        }

        @Override
        public boolean mayContainDuplicates() {
            return false;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        reader.close();
        directory.close();
    }
}
