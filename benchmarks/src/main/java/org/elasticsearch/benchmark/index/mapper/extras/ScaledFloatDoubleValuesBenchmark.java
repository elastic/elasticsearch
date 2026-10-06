/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper.extras;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.mapper.extras.ScaledFloatFieldMapper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures the cost of reading a singleton {@code scaled_float} field through
 * {@code ScaledFloatFieldMapper.ScaledFloatLeafFieldData#getDoubleValues()}.
 *
 * <p>Before the fix, that method always wrapped the full {@link SortedNumericLongValues} with
 * {@link SortedNumericDoubleValues.SortedNumericLongWrapper}, even for a singleton field. That wrapper
 * delegates every {@code advanceExact}/{@code nextValue} call to {@code SortedNumericLongValues.singleton(...)}'s
 * own wrapper, which in turn delegates to the underlying Lucene values: one extra virtual hop per value, on top
 * of what's otherwise already a tight read loop. The {@code beforeFix} benchmark reproduces that original shape
 * directly against {@link SortedNumericLongValues#getLongValues}, the same entry point the field mapper itself
 * uses, so it isn't a reimplementation of the logic, just the removed wrapping. The {@code afterFix} benchmark
 * calls the field mapper's current code, unmodified.
 *
 * <p>The extra hop is one JDK 27 made relatively more expensive: compact object headers (JEP 534, on by default
 * since JDK 27) pack the class pointer into the mark word, so devirtualizing a call that isn't fully inlined now
 * costs an extra decode. See https://github.com/elastic/elasticsearch-benchmarks/issues/3553.
 *
 * <p>Run both arms, ideally once per JDK/COH setting under comparison, e.g.:
 * <pre>
 *   cd benchmarks
 *   ../gradlew run --args "org.elasticsearch.benchmark.index.mapper.extras.ScaledFloatDoubleValuesBenchmark" \
 *     | tee /tmp/bench/scaled_float_double_values
 *   ../gradlew run --args "org.elasticsearch.benchmark.index.mapper.extras.ScaledFloatDoubleValuesBenchmark \
 *     -jvmArgsAppend -XX:-UseCompactObjectHeaders" | tee /tmp/bench/scaled_float_double_values_no_coh
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class ScaledFloatDoubleValuesBenchmark {

    private static final String FIELD = "amount";
    private static final double SCALING_FACTOR = 100d;
    private static final double SCALING_FACTOR_INVERSE = 1d / SCALING_FACTOR;
    private static final int NUM_DOCS = 500_000;

    private Directory dir;
    private DirectoryReader reader;
    private LeafReaderContext leaf;

    /** The field mapper's own (now fixed) field data, built exactly as {@code ScaledFloatFieldType} builds it. */
    private LeafNumericFieldData scaledLeafFieldData;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder().include(ScaledFloatDoubleValuesBenchmark.class.getSimpleName())
            .addProfiler(AsyncProfiler.class)
            .build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        dir = new ByteBuffersDirectory();
        Random random = new Random(42);
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < NUM_DOCS; i++) {
                long scaledValue = Math.round(random.nextDouble() * 1_000 * SCALING_FACTOR);
                Document doc = new Document();
                // scaled_float is indexed (points) and has doc values by default; mirror that so the
                // singleton field data takes the same "dense" path a real mapped field would.
                doc.add(new LongPoint(FIELD, scaledValue));
                doc.add(new SortedNumericDocValuesField(FIELD, scaledValue));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }

        reader = DirectoryReader.open(dir);
        leaf = reader.leaves().get(0);

        ScaledFloatFieldMapper.ScaledFloatFieldType fieldType = new ScaledFloatFieldMapper.ScaledFloatFieldType(FIELD, SCALING_FACTOR);
        IndexNumericFieldData indexFieldData = (IndexNumericFieldData) fieldType.fielddataBuilder(
            FieldDataContext.noRuntimeFields("index", "ScaledFloatDoubleValuesBenchmark")
        ).build(null, null);
        scaledLeafFieldData = indexFieldData.load(leaf);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        IOUtils.close(reader, dir);
    }

    /**
     * Reproduces the pre-fix shape: wrap the full {@link SortedNumericLongValues} unconditionally, even though
     * the field is a singleton. One extra virtual hop per {@code advanceExact}/{@code nextValue} call versus
     * {@link #afterFix()}.
     */
    @Benchmark
    @OperationsPerInvocation(NUM_DOCS)
    public long beforeFix() throws IOException {
        SortedNumericLongValues longValues = SortedNumericLongValues.getLongValues(FIELD, leaf.reader());
        SortedNumericDoubleValues values = new SortedNumericDoubleValues.SortedNumericLongWrapper(longValues) {
            @Override
            public double nextValue() throws IOException {
                return longValues.nextValue() * SCALING_FACTOR_INVERSE;
            }
        };
        return sumAllDocs(values);
    }

    /** The field mapper's current code: unwraps the singleton and reads straight off it. */
    @Benchmark
    @OperationsPerInvocation(NUM_DOCS)
    public long afterFix() throws IOException {
        return sumAllDocs(scaledLeafFieldData.getDoubleValues());
    }

    private long sumAllDocs(SortedNumericDoubleValues values) throws IOException {
        long bits = 0;
        int maxDoc = leaf.reader().maxDoc();
        for (int doc = 0; doc < maxDoc; doc++) {
            if (values.advanceExact(doc)) {
                int count = values.docValueCount();
                for (int i = 0; i < count; i++) {
                    bits += Double.doubleToRawLongBits(values.nextValue());
                }
            }
        }
        return bits;
    }
}
