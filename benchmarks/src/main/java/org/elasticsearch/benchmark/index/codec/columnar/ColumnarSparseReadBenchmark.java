/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * What sparse columns gain from taking the bulk and range fast paths instead of falling back to
 * per-document reads.
 *
 * <p>Each benchmark pairs a fast path against the fallback a caller used before it existed, on the same
 * segment and producing the same output: {@link #bulkLongs} against {@link #perDocPayload}, and
 * {@link #rangeIntoBitSet} against {@link #rangePayloadBitSet}, which is what
 * {@code ColumnarNumericRangeQuery} fell back to for a sparse column.
 * Density is swept because the answer is expected to depend on it — {@code IndexedDISI} reports long runs
 * for near-dense data and none at all for scattered data, and {@code density = 1.0} is the dense column,
 * present as a control that neither path should regress.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class ColumnarSparseReadBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    private static final String FIELD = "value";
    /** Documents handed to the bulk API at once, in the region of an ESQL page. */
    private static final int BATCH = 1024;
    private static final long RANGE_LOW = 400;
    private static final long RANGE_HIGH = 500;

    @Param({ "0.02", "0.1", "0.5", "0.9", "1.0" })
    private double density;

    @Param("200000")
    private int docCount;

    @Param("128")
    private int blockSize;

    private Path tempPath;
    private Directory directory;
    private DirectoryReader reader;
    private LeafReader leafReader;
    /** The documents that have a value, ascending — what a caller would ask for. */
    private int[] presentDocs;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        final Random random = new Random(17);
        final FieldType fieldType = new FieldType();
        fieldType.setDocValuesType(DocValuesType.BINARY);
        fieldType.putAttribute(ColumNARDocValuesFormat.TYPE_ATTRIBUTE, ColumnarFieldType.LONG.name());
        fieldType.freeze();

        tempPath = Files.createTempDirectory("columnar-sparse-");
        directory = FSDirectory.open(tempPath);
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(NumericFormat.COLUMNAR.codec("RANDOM_FULL", blockSize));
        final BytesRefBuilder builder = new BytesRefBuilder();
        int presentCount = 0;
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int doc = 0; doc < docCount; doc++) {
                final Document document = new Document();
                if (random.nextDouble() < density) {
                    final long value = random.nextInt(1000);
                    document.add(
                        new Field(FIELD, BytesRef.deepCopyOf(NumericBinaryPayload.encode(new long[] { value }, 1, builder)), fieldType)
                    );
                    presentCount++;
                }
                writer.addDocument(document);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        leafReader = reader.leaves().getFirst().reader();
        // Read the document ids back from the index rather than recording them while indexing: a merge of
        // non-adjacent segments leaves document ids in an order other than the one they were added in, so
        // the indexing loop's record does not necessarily describe the segment being measured.
        final int[] present = new int[presentCount];
        int found = 0;
        final BinaryDocValues docs = leafReader.getBinaryDocValues(FIELD);
        for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
            present[found++] = doc;
        }
        presentDocs = Arrays.copyOf(present, found);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        reader.close();
        directory.close();
        IOUtils.rm(tempPath);
    }

    private ColumnarNumericBinaryDocValues column() throws IOException {
        return (ColumnarNumericBinaryDocValues) leafReader.getBinaryDocValues(FIELD);
    }

    /** The fast path: batches of documents resolved to ordinals and sliced out of decoded blocks. */
    @Benchmark
    public void bulkLongs(Blackhole bh) throws IOException {
        final ColumnarNumericBinaryDocValues dv = column();
        final LongSink sink = new LongSink(bh);
        for (int offset = 0; offset < presentDocs.length; offset += BATCH) {
            final int count = Math.min(BATCH, presentDocs.length - offset);
            if (dv.bulkLongs(presentDocs, offset, count, false, sink) == false) {
                throw new AssertionError("bulk path declined");
            }
        }
    }

    /** The fallback: one advance and one payload decode per document. */
    @Benchmark
    public void perDocPayload(Blackhole bh) throws IOException {
        final BinaryDocValues dv = leafReader.getBinaryDocValues(FIELD);
        final long[][] decoded = { new long[8] };
        for (int doc : presentDocs) {
            if (dv.advanceExact(doc)) {
                final int count = NumericBinaryPayload.decode(dv.binaryValue(), decoded);
                for (int i = 0; i < count; i++) {
                    bh.consume(decoded[0][i]);
                }
            }
        }
    }

    /**
     * The fast path as a bulk scorer drives it: fill a bit set of matching documents in one call, which is
     * where the vectorized block mask, the run arithmetic and the skip index all pay off.
     */
    @Benchmark
    public void rangeIntoBitSet(Blackhole bh) throws IOException {
        final TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(column().rangeIterator(RANGE_LOW, RANGE_HIGH));
        final FixedBitSet matches = new FixedBitSet(docCount);
        if (twoPhase.approximation().nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            twoPhase.intoBitSet(docCount, matches, 0);
        }
        bh.consume(matches.cardinality());
    }

    /**
     * The fallback a sparse column used to get, producing the identical bit set: walk every document that
     * has a value, decode its payload, compare. Same allocation and same output as
     * {@link #rangeIntoBitSet}, so the two differ only in how the values are reached.
     */
    @Benchmark
    public void rangePayloadBitSet(Blackhole bh) throws IOException {
        final BinaryDocValues dv = leafReader.getBinaryDocValues(FIELD);
        final FixedBitSet matches = new FixedBitSet(docCount);
        final long[][] decoded = { new long[8] };
        for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
            final int count = NumericBinaryPayload.decode(dv.binaryValue(), decoded);
            for (int i = 0; i < count; i++) {
                final long value = decoded[0][i];
                if (value >= RANGE_LOW && value <= RANGE_HIGH) {
                    matches.set(doc);
                    break;
                }
            }
        }
        bh.consume(matches.cardinality());
    }

    /** The per-document scorer path, for reference against the bulk one above. */
    @Benchmark
    public void rangeNextDoc(Blackhole bh) throws IOException {
        final DocIdSetIterator disi = column().rangeIterator(RANGE_LOW, RANGE_HIGH);
        for (int doc = disi.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi.nextDoc()) {
            bh.consume(doc);
        }
    }

    private record LongSink(Blackhole bh) implements org.elasticsearch.columnar.numeric.LongBlockSink {
        @Override
        public void appendLongs(long[] values, int from, int length) {
            for (int i = 0; i < length; i++) {
                bh.consume(values[from + i]);
            }
        }
    }
}
