/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.flattened;

import org.apache.lucene.document.Document;
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
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;
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

/**
 * Read cost for the flattened {@code ._keyed} column over a single force-merged segment, under each
 * of the three {@link FlattenedKeyedFormat} arms. Two benchmarks measure complementary paths:
 *
 * <ul>
 *   <li>{@code fullRow} — iterates every doc and consumes its raw blob ({@link BinaryDocValues#binaryValue()}).
 *       For arm C this triggers {@code transposeBlock} on first access to each block, which
 *       rebuilds the per-doc row blobs from the columnar layout.</li>
 *   <li>{@code singleKey} — for every doc, scans the blob to find the first entry whose key matches
 *       a fixed target ({@code "key0"}), replicating the linear scan cost of
 *       {@code KeyedFlattenedBinaryDocValues.advanceExact} without the two-pass repositioning
 *       overhead. This is the metric that sub-chunked compression and a key-level API must beat.</li>
 * </ul>
 *
 * <p>The B→C delta on {@code singleKey} is the primary decision metric: if arm C already
 * shows a latency regression here (expected today, since the columnar block is decompressed in full
 * and transposed on every block touch), the magnitude sets the target for future optimizations.
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

    private static final String FIELD = "labels._keyed";

    @Param({ "ROW_SEPARATE_COUNT", "ROW_INTEGRATED_COUNT", "COLUMNAR_INTEGRATED_COUNT", "COLUMNAR_SUBCHUNKED" })
    private FlattenedKeyedFormat format;

    @Param({ "FEW_KEYS", "MEDIUM_KEYS", "MANY_KEYS" })
    private String workload;

    @Param("200000")
    private int numDocs;

    private Directory directory;
    private DirectoryReader reader;

    /**
     * Key search target: "key0\0" — present in nearly every doc across all workloads.
     * The null-byte terminator ensures the prefix does not accidentally match "key00", "key01", etc.
     */
    private byte[] keyWithSep;

    /** Reusable scratch buffer for blob parsing; avoids per-doc allocation in the scan benchmark. */
    private final ByteArrayStreamInput scratch = new ByteArrayStreamInput();

    @Setup(Level.Trial)
    public void setup() throws IOException {
        directory = FSDirectory.open(Files.createTempDirectory("flattened-keyed-read-"));
        List<List<BytesRef>> data = FlattenedKeyedData.generate(workload, numDocs);
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setCodec(format.codec()))) {
            for (int i = 0; i < numDocs; i++) {
                final Document doc = new Document();
                format.addField(doc, FIELD, data.get(i));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);

        byte[] keyBytes = FlattenedKeyedData.keyBytes(0);
        keyWithSep = new byte[keyBytes.length + 1];
        System.arraycopy(keyBytes, 0, keyWithSep, 0, keyBytes.length);
        keyWithSep[keyBytes.length] = 0; // separator
    }

    /**
     * Iterates every document and consumes its raw blob. Exercises block decompression (and, for
     * arm C, the full columnar transpose) amortized over all docs in a block.
     */
    @Benchmark
    public void fullRow(Blackhole bh) throws IOException {
        for (LeafReaderContext leaf : reader.leaves()) {
            final BinaryDocValues bdv = leaf.reader().getBinaryDocValues(FIELD);
            if (bdv == null) {
                continue;
            }
            while (bdv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                bh.consume(bdv.binaryValue());
            }
        }
    }

    /**
     * Iterates every document and scans its blob for a fixed key. The scan mirrors the linear pass
     * that the current read path performs and is what sub-chunked compression + a key-level API
     * would short-circuit. The scratch buffer is reused to avoid allocation.
     */
    @Benchmark
    public void singleKey(Blackhole bh) throws IOException {
        for (LeafReaderContext leaf : reader.leaves()) {
            final BinaryDocValues bdv = leaf.reader().getBinaryDocValues(FIELD);
            if (bdv == null) {
                continue;
            }
            while (bdv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                bh.consume(format.findKey(bdv.binaryValue(), keyWithSep, scratch));
            }
        }
    }

    /**
     * Like {@code singleKey} but for {@code COLUMNAR_SUBCHUNKED} takes the fast path: casts the
     * leaf's {@link BinaryDocValues} to
     * {@link AbstractTSDBDocValuesProducer.SubchunkedBinaryDocValues} and calls
     * {@code lookupKey(keyWithSep)}, which decompresses only the target key's run. For other
     * formats falls back to the same linear blob scan as {@link #singleKey}.
     */
    @Benchmark
    public void singleKeyDirect(Blackhole bh) throws IOException {
        for (LeafReaderContext leaf : reader.leaves()) {
            final BinaryDocValues bdv = leaf.reader().getBinaryDocValues(FIELD);
            if (bdv == null) {
                continue;
            }
            if (format.isSubchunked() && bdv instanceof AbstractTSDBDocValuesProducer.SubchunkedBinaryDocValues sub) {
                while (sub.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    bh.consume(sub.lookupKey(keyWithSep));
                }
            } else {
                while (bdv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    bh.consume(format.findKey(bdv.binaryValue(), keyWithSep, scratch));
                }
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        reader.close();
        directory.close();
    }
}
