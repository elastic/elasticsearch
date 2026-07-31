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
import org.apache.lucene.document.Document;
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

/**
 * Ingest cost and on-disk footprint for the flattened {@code ._keyed} column under each of the
 * three {@link FlattenedKeyedFormat} arms. The secondary counters report the final on-disk size and
 * the total bytes written including merged-away segments.
 *
 * <p>Key comparisons:
 * <ul>
 *   <li>A→B ({@code ROW_SEPARATE_COUNT} vs {@code ROW_INTEGRATED_COUNT}): framing change only.</li>
 *   <li>B→C ({@code ROW_INTEGRATED_COUNT} vs {@code COLUMNAR_INTEGRATED_COUNT}): block layout
 *       change only. The B→C size delta is what decides whether the columnar path is worth
 *       completing with sub-chunked compression.</li>
 * </ul>
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

    private static final String FIELD = "labels._keyed";

    public enum Merge {
        NONE,
        NATURAL,
        FORCE
    }

    @Param({ "ROW_SEPARATE_COUNT", "ROW_INTEGRATED_COUNT", "COLUMNAR_INTEGRATED_COUNT" })
    private FlattenedKeyedFormat format;

    @Param({ "FEW_KEYS", "MEDIUM_KEYS", "MANY_KEYS" })
    private String workload;

    @Param({ "NONE", "NATURAL", "FORCE" })
    private Merge merge;

    @Param("200000")
    private int docCount;

    private List<List<BytesRef>> data;
    private Codec codec;

    @Setup(Level.Trial)
    public void setup() {
        data = FlattenedKeyedData.generate(workload, docCount);
        codec = format.codec();
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
                final Document doc = new Document();
                format.addField(doc, FIELD, data.get(i));
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
