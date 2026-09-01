/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * A numeric range query over a single force-merged segment, split into {@code numSlices} contiguous
 * doc-id windows each scored by its own fresh {@link org.apache.lucene.search.BulkScorer}, mirroring
 * ESQL {@code DataPartitioning.DOC}. Set {@code format} to compare ColumNAR's range path against the
 * TSDB codecs on identical data.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class ColumnarNumericRangeSlicingBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    private static final String FIELD = NumericFormat.FIELD;

    @Param({ "LUCENE", "ES819", "ES95", "COLUMNAR" })
    private NumericFormat format;

    @Param("1000000")
    private int numDocs;

    @Param({ "1", "64", "1024" })
    private int numSlices;

    @Param({ "MONOTONIC_TIMESTAMPS", "RANDOM_FULL" })
    private String workload;

    @Param({ "128", "512" })
    private int blockSize;

    // 0.0 is the near-empty worst case
    @Param({ "0.0", "0.001" })
    private double selectivity;

    private Directory directory;
    private DirectoryReader reader;
    private Weight weight;
    private int maxDoc;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        final long[] values = NumericData.generate(workload, numDocs);
        directory = format.buildSegment(FIELD, workload, values, "columnar-range-slicing-", blockSize);
        reader = DirectoryReader.open(directory);
        maxDoc = reader.maxDoc();

        // Range chosen by rank so selectivity is exact and value-span overflow is avoided.
        final long[] sorted = values.clone();
        Arrays.sort(sorted);
        final int loRank = numDocs / 2;
        final int hiRank = Math.min(numDocs - 1, loRank + (int) (numDocs * selectivity));
        final Query query = format.rangeQuery(FIELD, sorted[loRank], sorted[hiRank]);
        final IndexSearcher searcher = new IndexSearcher(reader);
        // createWeight wraps the weight in the query cache whenever scores are not needed, and this weight
        // is reused by every invocation, so the cache would serve a stored bit set instead of running the
        // format's range path.
        searcher.setQueryCache(null);
        weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
    }

    @Benchmark
    public void sliceAndScore(Blackhole bh) throws IOException {
        final LeafReaderContext leaf = reader.leaves().getFirst();
        final CountingCollector collector = new CountingCollector();
        final int sliceSize = Math.max(1, (maxDoc + numSlices - 1) / numSlices);
        for (int min = 0; min < maxDoc; min += sliceSize) {
            final int max = Math.min(min + sliceSize, maxDoc);
            final ScorerSupplier scorerSupplier = weight.scorerSupplier(leaf);
            if (scorerSupplier == null) {
                continue;
            }
            scorerSupplier.bulkScorer().score(collector, leaf.reader().getLiveDocs(), min, max);
        }
        bh.consume(collector.count);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        reader.close();
        directory.close();
    }

    private static final class CountingCollector implements LeafCollector {
        private long count;

        @Override
        public void setScorer(Scorable scorer) {}

        @Override
        public void collect(int doc) {
            count++;
        }
    }
}
