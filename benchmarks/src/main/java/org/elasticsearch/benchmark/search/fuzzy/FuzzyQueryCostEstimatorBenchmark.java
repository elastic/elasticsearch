/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.fuzzy;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.lucene.search.cost.FuzzyQueryCostEstimator;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@SuppressWarnings("unused") // invoked by JMH
public class FuzzyQueryCostEstimatorBenchmark {

    static {
        LogConfigurator.setNodeName("benchmark");
    }

    static final String FIELD = "f";

    /** ASCII letters/digits used to synthesise within-edit-distance neighbours of the query term. */
    private static final String REPLACEMENT_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /** Upper bound on the seeded vocabulary size; comfortably above the largest {@code maxExpansions}. */
    private static final int MAX_VOCABULARY = 1024;

    public enum Alphabet {
        SINGLE_CHAR {
            @Override
            String generate(int n, Random r) {
                return "a".repeat(n);
            }
        },
        ASCII_LETTERS {
            @Override
            String generate(int n, Random r) {
                StringBuilder sb = new StringBuilder(n);
                for (int i = 0; i < n; i++) {
                    sb.append((char) ('a' + r.nextInt(26)));
                }
                return sb.toString();
            }
        },
        UNICODE_BMP {
            @Override
            String generate(int n, Random r) {
                StringBuilder sb = new StringBuilder(n);
                for (int i = 0; i < n; i++) {
                    int cp;
                    do {
                        cp = r.nextInt(0xD800);
                    } while (Character.isISOControl(cp) || Character.isWhitespace(cp));
                    sb.appendCodePoint(cp);
                }
                return sb.toString();
            }
        };

        abstract String generate(int n, Random r);
    }

    public enum TermFanout {
        ALL,
        SPARSE
    }

    /** Number of segments a term is written into under {@link TermFanout#SPARSE}, capped at {@link #segments}. */
    private static final int SPARSE_SEGMENTS_PER_TERM = 2;

    @Param({ "5", "20", "50", "200", "1024" })
    public int termLength;

    @Param({ "1", "2" })
    public int maxEdits;

    @Param({ "0", "3" })
    public int prefixLength;

    @Param({ "true", "false" })
    public boolean transpositions;

    @Param({ "SINGLE_CHAR", "ASCII_LETTERS", "UNICODE_BMP" })
    public Alphabet alphabet;

    @Param({ "10", "50", "200" })
    public int maxExpansions;

    @Param({ "1", "4", "16", "64", "128" })
    public int segments;

    @Param({ "ALL", "SPARSE" })
    public TermFanout termFanout;

    private String term;
    private int termByteLength;
    private int distinctUtf8Bytes;

    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private FuzzyQuery fuzzyQuery;

    private long precomputedEstimate;
    private long precomputedAutomaton;
    private long precomputedRewrite;
    private long precomputedMeasured;
    private int precomputedExpandedTerms;
    private double precomputedRatio;

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Metrics {
        public double estimatedBytes;
        public double automatonBytes;
        public double rewriteBytes;
        public double measuredBytes;
        public double expandedTerms;
        public double estimateOverMeasuredRatio;
    }

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        if (prefixLength > termLength) {
            term = null;
            return;
        }
        Random rnd = new Random(0xC0FFEEL ^ termLength ^ alphabet.ordinal());
        term = alphabet.generate(termLength, rnd);
        byte[] utf8 = term.getBytes(StandardCharsets.UTF_8);
        termByteLength = utf8.length;
        distinctUtf8Bytes = countDistinctUtf8Bytes(utf8);

        directory = new ByteBuffersDirectory();
        List<String> vocabulary = new ArrayList<>(buildVocabulary(term, prefixLength));
        if (vocabulary.isEmpty()) {
            throw new IllegalStateException("empty vocabulary for term length " + termLength + " / prefix " + prefixLength);
        }

        int segmentsPerTerm = termFanout == TermFanout.ALL ? segments : SPARSE_SEGMENTS_PER_TERM;
        List<Set<String>> vocabularyBySegment = assignVocabularyToSegments(vocabulary, segments, segmentsPerTerm);

        IndexWriterConfig writerConfig = new IndexWriterConfig(null).setMergePolicy(NoMergePolicy.INSTANCE).setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(directory, writerConfig)) {
            for (int s = 0; s < segments; s++) {
                for (String neighbour : vocabularyBySegment.get(s)) {
                    Document doc = new Document();
                    doc.add(new StringField(FIELD, neighbour, Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.flush();
            }
            writer.commit();
        }
        reader = DirectoryReader.open(directory);
        if (reader.leaves().size() != segments) {
            throw new IllegalStateException(
                "expected index with " + segments + " segments but reader has " + reader.leaves().size() + " leaves"
            );
        }
        searcher = new IndexSearcher(reader);

        fuzzyQuery = new FuzzyQuery(new Term(FIELD, term), maxEdits, prefixLength, maxExpansions, transpositions);

        precomputedEstimate = new FuzzyQueryCostEstimator(
            termByteLength,
            distinctUtf8Bytes,
            maxEdits,
            prefixLength,
            maxExpansions,
            segments
        ).estimate();
        precomputedAutomaton = sumRamBytes(term, maxEdits, prefixLength, transpositions);
        Set<Term> expanded = collectExpandedTerms(searcher, fuzzyQuery);
        precomputedExpandedTerms = expanded.size();
        precomputedRewrite = measureRewriteRam(searcher, fuzzyQuery, expanded);
        precomputedMeasured = precomputedAutomaton + precomputedRewrite;
        precomputedRatio = precomputedMeasured == 0 ? 0.0 : (double) precomputedEstimate / (double) precomputedMeasured;
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        if (directory != null) {
            directory.close();
            directory = null;
        }
    }

    @Benchmark
    public long estimate(Metrics metrics) {
        if (term == null) {
            return 0L;
        }
        publish(metrics);
        return new FuzzyQueryCostEstimator(termByteLength, distinctUtf8Bytes, maxEdits, prefixLength, maxExpansions, segments).estimate();
    }

    @Benchmark
    public long measureAutomaton(Metrics metrics) {
        if (term == null) {
            return 0L;
        }
        publish(metrics);
        return sumRamBytes(term, maxEdits, prefixLength, transpositions);
    }

    @Benchmark
    public long measureRewrite(Metrics metrics) throws IOException {
        if (term == null) {
            return 0L;
        }
        publish(metrics);
        return measureRewriteRam(searcher, fuzzyQuery, collectExpandedTerms(searcher, fuzzyQuery));
    }

    private void publish(Metrics metrics) {
        metrics.estimatedBytes = precomputedEstimate;
        metrics.automatonBytes = precomputedAutomaton;
        metrics.rewriteBytes = precomputedRewrite;
        metrics.measuredBytes = precomputedMeasured;
        metrics.expandedTerms = precomputedExpandedTerms;
        metrics.estimateOverMeasuredRatio = precomputedRatio;
    }

    private static long sumRamBytes(String term, int maxEdits, int prefixLength, boolean transpositions) {
        long sum = 0L;
        for (int e = 0; e <= maxEdits; e++) {
            CompiledAutomaton ca = FuzzyQuery.getFuzzyAutomaton(term, e, prefixLength, transpositions);
            sum += ca.ramBytesUsed();
        }
        return sum;
    }

    private static Set<Term> collectExpandedTerms(IndexSearcher searcher, FuzzyQuery query) throws IOException {
        Query rewritten = searcher.rewrite(query);
        Set<Term> terms = new LinkedHashSet<>();
        rewritten.visit(new QueryVisitor() {
            @Override
            public void consumeTerms(Query q, Term... ts) {
                Collections.addAll(terms, ts);
            }

            @Override
            public boolean acceptField(String field) {
                return true;
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                return this;
            }
        });
        return terms;
    }

    private static long measureRewriteRam(IndexSearcher searcher, FuzzyQuery query, Set<Term> expanded) throws IOException {
        long total = RamUsageEstimator.sizeOf(searcher.rewrite(query));
        for (Term term : expanded) {
            total += measureTermStatesRam(searcher, term);
        }
        return total;
    }

    private static long measureTermStatesRam(IndexSearcher searcher, Term term) throws IOException {
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        TermStates termStates = TermStates.build(searcher, term, true);
        long total = RamUsageEstimator.shallowSizeOf(termStates) + RamUsageEstimator.shallowSizeOf(new TermState[leaves.size()]);
        for (LeafReaderContext ctx : leaves) {
            IOSupplier<TermState> supplier = termStates.get(ctx);
            if (supplier == null) {
                // Cheap negative check: the term is absent from this leaf, so no TermState is retained for it.
                continue;
            }
            TermState state = supplier.get();
            if (state != null) {
                total += RamUsageEstimator.shallowSizeOf(state);
            }
        }
        return total;
    }

    private static List<Set<String>> assignVocabularyToSegments(List<String> vocabulary, int segments, int segmentsPerTerm) {
        List<Set<String>> vocabularyBySegment = new ArrayList<>(segments);
        for (int s = 0; s < segments; s++) {
            vocabularyBySegment.add(new LinkedHashSet<>());
        }
        int fanout = Math.min(segments, segmentsPerTerm);
        for (int i = 0; i < vocabulary.size(); i++) {
            String neighbour = vocabulary.get(i);
            for (int j = 0; j < fanout; j++) {
                vocabularyBySegment.get((i + j) % segments).add(neighbour);
            }
        }
        return vocabularyBySegment;
    }

    private static Set<String> buildVocabulary(String term, int prefixLength) {
        Set<String> vocabulary = new LinkedHashSet<>();
        int length = term.length();
        // Substitutions in the fuzzy suffix.
        for (int pos = prefixLength; pos < length && vocabulary.size() < MAX_VOCABULARY; pos++) {
            for (int i = 0; i < REPLACEMENT_ALPHABET.length() && vocabulary.size() < MAX_VOCABULARY; i++) {
                char c = REPLACEMENT_ALPHABET.charAt(i);
                if (c != term.charAt(pos)) {
                    vocabulary.add(term.substring(0, pos) + c + term.substring(pos + 1));
                }
            }
        }
        // Insertions in the fuzzy suffix (produces length+1 neighbours).
        for (int pos = prefixLength; pos <= length && vocabulary.size() < MAX_VOCABULARY; pos++) {
            for (int i = 0; i < REPLACEMENT_ALPHABET.length() && vocabulary.size() < MAX_VOCABULARY; i++) {
                char c = REPLACEMENT_ALPHABET.charAt(i);
                vocabulary.add(term.substring(0, pos) + c + term.substring(pos));
            }
        }
        // Deletions in the fuzzy suffix (produces length-1 neighbours).
        for (int pos = prefixLength; pos < length && vocabulary.size() < MAX_VOCABULARY; pos++) {
            vocabulary.add(term.substring(0, pos) + term.substring(pos + 1));
        }
        vocabulary.remove(term);
        return vocabulary;
    }

    private static int countDistinctUtf8Bytes(byte[] utf8) {
        BitSet seen = new BitSet(256);
        for (byte b : utf8) {
            seen.set(b & 0xff);
        }
        return seen.cardinality();
    }
}
