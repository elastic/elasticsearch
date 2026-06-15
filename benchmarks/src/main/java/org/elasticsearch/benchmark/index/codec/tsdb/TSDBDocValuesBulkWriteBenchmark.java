/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormatFactory;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormatFactory;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Bulk write benchmark: indexes a fixed number of documents with one sorted numeric
 * doc values field and commits once at the end. Runs entirely in memory on a
 * {@code ByteBuffersDirectory} with merging disabled, so the measurement isolates
 * the encoder CPU path of each TSDB doc values format from disk IO and merge work.
 *
 * <p>Per field allocation differences are largely amortized over a single large
 * segment, so this benchmark is the place to look for per block encoder differences
 * (numeric stages, ordinal blocks, terms dict) rather than per segment fixed overhead.
 * For the per segment overhead, see {@code TSDBDocValuesSmallSegmentWriteBenchmark}.
 *
 * <h2>Ready to run command</h2>
 *
 * <pre>{@code
 * ./gradlew :benchmarks:run --args="TSDBDocValuesBulkWriteBenchmark \
 *   -f 3 -wi 3 -i 5 -prof gc"
 * }</pre>
 */
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class TSDBDocValuesBulkWriteBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final String FIELD_NAME = "metric";

    @Param({ "100000" })
    private int docCount;

    private long[] values;
    private Codec es819Codec;
    private Codec es95Codec;
    private Codec es95UncachedCodec;

    private Directory es819Directory;
    private IndexWriter es819Writer;
    private Directory es95Directory;
    private IndexWriter es95Writer;
    private Directory es95UncachedDirectory;
    private IndexWriter es95UncachedWriter;

    @Setup(Level.Trial)
    public void setupTrial() {
        final DocValuesFormat es819Format = ES819TSDBDocValuesFormatFactory.createDocValuesFormat(
            IndexVersion.current(),
            false,
            false,
            false
        );
        final DocValuesFormat es95Format = ES95TSDBDocValuesFormatFactory.create(false, false, false, null);
        es819Codec = wrapCodec(es819Format);
        es95Codec = wrapCodec(es95Format);
        es95UncachedCodec = wrapUncachedES95Codec();

        final Random random = new Random(42L);
        values = new long[docCount];
        for (int i = 0; i < docCount; i++) {
            values[i] = random.nextInt(256) - 128;
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        es819Directory = new ByteBuffersDirectory();
        es819Writer = newWriter(es819Directory, es819Codec);
        es95Directory = new ByteBuffersDirectory();
        es95Writer = newWriter(es95Directory, es95Codec);
        es95UncachedDirectory = new ByteBuffersDirectory();
        es95UncachedWriter = newWriter(es95UncachedDirectory, es95UncachedCodec);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() throws IOException {
        IOUtils.close(es819Writer, es819Directory, es95Writer, es95Directory, es95UncachedWriter, es95UncachedDirectory);
        es819Writer = null;
        es819Directory = null;
        es95Writer = null;
        es95Directory = null;
        es95UncachedWriter = null;
        es95UncachedDirectory = null;
    }

    @Benchmark
    public void bulkWriteES819DocValuesFormat() throws IOException {
        writeAll(es819Writer);
    }

    @Benchmark
    public void bulkWriteES95DocValuesFormat() throws IOException {
        writeAll(es95Writer);
    }

    @Benchmark
    public void bulkWriteES95UncachedDocValuesFormat() throws IOException {
        writeAll(es95UncachedWriter);
    }

    private void writeAll(final IndexWriter writer) throws IOException {
        final Document doc = new Document();
        final SortedNumericDocValuesField field = new SortedNumericDocValuesField(FIELD_NAME, 0L);
        doc.add(field);
        for (int i = 0; i < docCount; i++) {
            field.setLongValue(values[i]);
            writer.addDocument(doc);
        }
        writer.commit();
    }

    private static Codec wrapCodec(final DocValuesFormat dvFormat) {
        return new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return dvFormat;
            }
        };
    }

    private static Codec wrapUncachedES95Codec() {
        // NOTE: allocates a fresh format on every Lucene `getDocValuesFormatForField`
        // call, mirroring what would happen without the per-supplier cache in
        // `PerFieldFormatSupplier`. The other ES95 variant in this benchmark reuses
        // one format instance across all calls (the production behavior).
        return new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return ES95TSDBDocValuesFormatFactory.create(false, false, false, null);
            }
        };
    }

    private static IndexWriter newWriter(final Directory directory, final Codec codec) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setCodec(codec);
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        return new IndexWriter(directory, config);
    }
}
