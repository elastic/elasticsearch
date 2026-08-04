/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.flattened;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull;
import org.openjdk.jmh.annotations.AuxCounters;
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
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.MAX_BUFFERED_BYTES_DEFAULT;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.MAX_DOCS_PER_BLOCK_DEFAULT;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.MIN_COMPRESS_BYTES_DEFAULT;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.TARGET_BLOCK_BYTES_DEFAULT;

/**
 * Ingest cost and on-disk footprint of the flattened {@code ._keyed} column under two layouts.
 *
 * <ul>
 *   <li>{@code ROW} — uses {@link ES95TSDBDocValuesFormat}: all sub-field key-value pairs for a
 *       document are concatenated into a single binary blob, compressed per block.</li>
 *   <li>{@code COLUMNAR} — uses {@link FlattenedDocValuesFormat}: each sub-field's values are
 *       compressed independently, so reading one sub-field decompresses only that field's run.</li>
 * </ul>
 *
 * <p>The secondary {@link Bytes} counters report the final on-disk size and the total bytes written
 * across all segments (including any merged away). Sweep {@code workload} to compare storage at
 * different key-cardinalities; sweep {@code merge} to isolate flush cost from merge amortisation.
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class FlattenedKeyedIngestBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    static final String KEYED_FIELD = "labels._keyed";

    public enum Layout {
        ROW,
        COLUMNAR;

        /**
         * Builds a codec that routes {@link FlattenedKeyedIngestBenchmark#KEYED_FIELD} through
         * the appropriate doc-values format.  For {@code COLUMNAR}, {@code targetBlockBytes} and
         * {@code maxDocsPerBlock} are forwarded to {@link FlattenedDocValuesFormat}; for {@code ROW}
         * they are ignored (the TSDB format has its own fixed thresholds).
         */
        Codec codec(int targetBlockBytes, int maxDocsPerBlock) {
            final DocValuesFormat dvFormat = switch (this) {
                case ROW -> new ES95TSDBDocValuesFormat();
                case COLUMNAR -> new FlattenedDocValuesFormat(
                    targetBlockBytes,
                    maxDocsPerBlock,
                    MIN_COMPRESS_BYTES_DEFAULT,
                    MAX_BUFFERED_BYTES_DEFAULT
                );
            };
            return new Elasticsearch93Lucene104Codec() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return KEYED_FIELD.equals(field) ? dvFormat : super.getDocValuesFormatForField(field);
                }
            };
        }
    }

    public enum Merge {
        NONE,
        NATURAL,
        FORCE
    }

    @Param({ "ROW", "COLUMNAR" })
    private Layout layout;

    @Param({ "FEW_KEYS", "MEDIUM_KEYS", "MANY_KEYS", "HIGH_CARDINALITY" })
    private String workload;

    @Param({ "NONE", "NATURAL", "FORCE" })
    private Merge merge;

    @Param("200000")
    private int docCount;

    /** Target uncompressed bytes per block (COLUMNAR only). Default matches the production default. */
    @Param("" + TARGET_BLOCK_BYTES_DEFAULT)
    private int targetBlockBytes;

    /** Maximum documents per block (COLUMNAR only). Default matches the production default. */
    @Param("" + MAX_DOCS_PER_BLOCK_DEFAULT)
    private int maxDocsPerBlock;

    private List<List<BytesRef>> data;
    private Codec codec;

    @Setup(Level.Trial)
    public void setup() {
        data = FlattenedKeyedData.generate(workload, docCount);
        codec = layout.codec(targetBlockBytes, maxDocsPerBlock);
    }

    /** Secondary metrics: bytes on disk at the end, and total bytes written across all segments. */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Bytes {
        public double bytesOnDisk;
        public double bytesWritten;

        @Setup(Level.Iteration)
        public void reset() {
            bytesOnDisk = 0;
            bytesWritten = 0;
        }
    }

    @Benchmark
    public void index(Bytes bytes) throws IOException {
        final CountingDirectory directory = new CountingDirectory(new ByteBuffersDirectory());
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(codec);
        if (merge == Merge.NONE) {
            config.setMergePolicy(NoMergePolicy.INSTANCE);
        }
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < docCount; i++) {
                final LuceneDocument doc = new LuceneDocument();
                for (BytesRef kv : data.get(i)) {
                    KeyedArrayOrderInlineNull.recordValue(doc, KEYED_FIELD, kv);
                }
                writer.addDocument(doc);
            }
            if (merge == Merge.FORCE) {
                writer.forceMerge(1);
            }
        }
        long onDisk = 0;
        for (String file : directory.listAll()) {
            onDisk += directory.fileLength(file);
        }
        bytes.bytesOnDisk = onDisk;
        bytes.bytesWritten = directory.bytesWritten;
        directory.close();
    }

    /** Wraps a directory to total the bytes written across every output, including files later merged away. */
    private static final class CountingDirectory extends FilterDirectory {
        private long bytesWritten;

        CountingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            return count(super.createOutput(name, context));
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            return count(super.createTempOutput(prefix, suffix, context));
        }

        private IndexOutput count(IndexOutput out) {
            return new FilterIndexOutput("counting", out.getName(), out) {
                @Override
                public void close() throws IOException {
                    bytesWritten += getFilePointer();
                    super.close();
                }
            };
        }
    }
}
