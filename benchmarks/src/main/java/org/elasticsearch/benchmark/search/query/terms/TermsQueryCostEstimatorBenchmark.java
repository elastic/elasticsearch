/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.query.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.lucene.search.cost.TermsQueryCostEstimator;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Compares {@link TermsQueryCostEstimator}'s circuit-breaker estimate for a {@link TermInSetQuery}
 * against the RAM the real query retains, split into a structural (query object) and a per-leaf
 * execution ({@code DocIdSet}) cost. The {@link Metrics} aux counters are JMH {@code EVENTS}, so
 * divide each by {@code Cnt} to recover absolute bytes.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@SuppressWarnings("unused") // invoked by JMH
public class TermsQueryCostEstimatorBenchmark {

    static {
        LogConfigurator.setNodeName("benchmark");
    }

    static final String FIELD = "f";

    /** Mirrors {@code MultiTermQueryConstantScoreBlendedWrapper.POSTINGS_PRE_PROCESS_THRESHOLD}. */
    static final int POSTINGS_PRE_PROCESS_THRESHOLD = 512;

    /** Mirrors {@code AbstractMultiTermQueryConstantScoreWrapper.BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD}. */
    static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

    @Param({ "1000000" })
    public int nDocs;

    /** Distinct terms indexed round-robin; sets the per-term doc frequency ({@code nDocs / cardinality}). */
    @Param({ "1024", "100000" })
    public int cardinality;

    /** Fraction of the distinct terms the query selects. */
    @Param({ "1.0", "0.1" })
    public double matchFraction;

    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private TermInSetQuery query;
    private List<BytesRef> selectedTerms;

    private long structuralEstimate;
    private long structuralMeasured;
    private long executionEstimate;
    private long executionMeasured;
    private long totalEstimate;
    private long totalMeasured;
    private double totalRatio;
    private double executionRatio;

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Metrics {
        public double estimatedBytes;
        public double measuredBytes;
        public double estimateOverMeasuredRatio;
        public double executionEstimatedBytes;
        public double executionMeasuredBytes;
        public double executionEstimateOverMeasuredRatio;
    }

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        directory = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            for (int docId = 0; docId < nDocs; docId++) {
                Document doc = new Document();
                doc.add(new StringField(FIELD, term(docId % cardinality), Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);

        int selectedCount = Math.min(cardinality, Math.max(1, (int) Math.round(cardinality * matchFraction)));
        selectedTerms = new ArrayList<>(selectedCount);
        for (int i = 0; i < selectedCount; i++) {
            selectedTerms.add(new BytesRef(term(i)));
        }
        query = new TermInSetQuery(FIELD, selectedTerms);

        structuralEstimate = new TermsQueryCostEstimator(query.ramBytesUsed()).estimate();
        structuralMeasured = query.ramBytesUsed();
        executionEstimate = estimateExecutionRam(searcher, reader, query);
        executionMeasured = measureExecutionRam(reader, selectedTerms);
        totalEstimate = structuralEstimate + executionEstimate;
        totalMeasured = structuralMeasured + executionMeasured;
        totalRatio = totalMeasured == 0 ? 0.0 : (double) totalEstimate / (double) totalMeasured;
        executionRatio = executionMeasured == 0 ? 0.0 : (double) executionEstimate / (double) executionMeasured;
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (directory != null) {
            directory.close();
        }
    }

    @Benchmark
    public long estimate(Metrics metrics) throws IOException {
        publish(metrics);
        return structuralEstimate + estimateExecutionRam(searcher, reader, query);
    }

    @Benchmark
    public long measure(Metrics metrics) throws IOException {
        publish(metrics);
        return structuralMeasured + measureExecutionRam(reader, selectedTerms);
    }

    private void publish(Metrics metrics) {
        metrics.estimatedBytes = totalEstimate;
        metrics.measuredBytes = totalMeasured;
        metrics.estimateOverMeasuredRatio = totalRatio;
        metrics.executionEstimatedBytes = executionEstimate;
        metrics.executionMeasuredBytes = executionMeasured;
        metrics.executionEstimateOverMeasuredRatio = executionRatio;
    }

    /** The breaker estimate: {@link TermsQueryCostEstimator#executionBytesForLeaf} per leaf, as {@code MultiTermBreakerWeight} charges it. */
    private static long estimateExecutionRam(IndexSearcher searcher, DirectoryReader reader, TermInSetQuery query) throws IOException {
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        long total = 0L;
        for (LeafReaderContext leaf : reader.leaves()) {
            ScorerSupplier scorerSupplier = weight.scorerSupplier(leaf);
            if (scorerSupplier == null) {
                continue;
            }
            total += TermsQueryCostEstimator.executionBytesForLeaf(scorerSupplier.cost(), leaf.reader().maxDoc());
        }
        return total;
    }

    /** The real per-leaf {@link DocIdSet} RAM the blended rewrite materialises, summed over segments. */
    private static long measureExecutionRam(DirectoryReader reader, List<BytesRef> selectedTerms) throws IOException {
        long total = 0L;
        for (LeafReaderContext leaf : reader.leaves()) {
            total += measureLeafExecutionRam(leaf, selectedTerms);
        }
        return total;
    }

    private static long measureLeafExecutionRam(LeafReaderContext leaf, List<BytesRef> selectedTerms) throws IOException {
        Terms terms = leaf.reader().terms(FIELD);
        if (terms == null) {
            return 0L;
        }
        int fieldDocCount = terms.getDocCount();
        TermsEnum seeker = terms.iterator();
        List<BytesRef> lowFrequency = new ArrayList<>();
        List<BytesRef> highFrequency = new ArrayList<>();
        for (BytesRef term : selectedTerms) {
            if (seeker.seekExact(term) == false) {
                continue;
            }
            int docFreq = seeker.docFreq();
            if (docFreq == fieldDocCount) {
                // A term matching every doc with a value short-circuits to a single TermQuery: no result bit set.
                return 0L;
            }
            if (docFreq <= POSTINGS_PRE_PROCESS_THRESHOLD) {
                lowFrequency.add(BytesRef.deepCopyOf(term));
            } else {
                highFrequency.add(BytesRef.deepCopyOf(term));
            }
        }
        int presentCount = lowFrequency.size() + highFrequency.size();
        if (presentCount <= BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD) {
            // Rewritten as a plain disjunction of TermQuerys; no result bit set is materialised.
            return 0L;
        }
        DocIdSetBuilder builder = new DocIdSetBuilder(leaf.reader().maxDoc(), terms);
        for (BytesRef term : lowFrequency) {
            addPostings(builder, terms, term);
        }
        // The blended rewrite keeps the costliest terms merged live; only the overflow lands in the bit set.
        int mergedLive = Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, highFrequency.size());
        for (int i = mergedLive; i < highFrequency.size(); i++) {
            addPostings(builder, terms, highFrequency.get(i));
        }
        return builder.build().ramBytesUsed();
    }

    private static void addPostings(DocIdSetBuilder builder, Terms terms, BytesRef term) throws IOException {
        TermsEnum termsEnum = terms.iterator();
        if (termsEnum.seekExact(term)) {
            PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
            builder.add(postings);
        }
    }

    private static String term(int ordinal) {
        return String.format(Locale.ROOT, "t%08d", ordinal);
    }
}
