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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.logging.LogConfigurator;
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares two ways of matching an integer {@code terms} query as the size of the value list
 * grows: the current default (BKD points, {@link IntPoint#newSetQuery}) against the
 * {@code index_terms}-mapped path (a sortable-bytes term in the terms dictionary, matched with
 * {@link TermInSetQuery}) that {@code NumberFieldMapper} uses when {@code index_terms} is enabled
 * on an {@code integer} field.
 * <p>
 * The field/query construction mirrors {@code NumberFieldMapper}'s private {@code IndexTermsIntegerField},
 * {@code encodeIndexTerm}, and {@code INDEX_TERMS_FIELD_TYPE} (points path) / the {@code indexTerms}
 * branch of {@code termsQuery} (terms path); those are not reachable from this module, so the
 * equivalent Lucene-level construction is duplicated here and should be kept in sync if the mapper
 * changes.
 * <p>
 * {@code buildQuery} isolates the cost of turning {@code nTerms} values into a {@link Query} (sorting,
 * dedup, packing); {@code search} isolates the cost of matching a pre-built query against the index.
 * Both matter: a large raw values list pays the construction cost on every request, while match cost
 * is what governs how the two approaches scale as {@code nTerms} grows towards and past the default
 * {@code index.max_terms_count} (65536) — a limit this benchmark deliberately ignores since it lives
 * in {@code TermsQueryBuilder}, above the Lucene query layer exercised here.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@SuppressWarnings("unused") // invoked by JMH
public class IntegerTermsQueryBenchmark {

    static {
        // IndexWriterConfig below picks up Elasticsearch814Codec via Lucene's codec SPI (since
        // :server is on the classpath), whose static init reaches Elasticsearch's log4j config.
        // That config's %node_name converter throws unless a node name was set, which never
        // happens in a bare JMH JVM. Set a dummy one before anything can trigger that lookup.
        LogConfigurator.setNodeName("benchmark");
    }

    static final String FIELD = "f";
    static final int N_DOCS = 10_000_000;
    static final int TOP_N = 10;

    static int[] distinctRandomInts(Random random, int count, int bound) {
        BitSet seen = new BitSet(bound);
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            int value;
            do {
                value = random.nextInt(bound);
            } while (seen.get(value));
            seen.set(value);
            values[i] = value;
        }
        return values;
    }

    /**
     * Mirrors {@code NumberFieldMapper#encodeIndexTerm}: encodes {@code value} as the same
     * sortable-bytes term {@link IntPoint} uses for BKD points, so unsigned byte-wise term order
     * matches numeric order (see {@link NumericUtils#intToSortableBytes}).
     */
    static BytesRef encodeIndexTerm(int value) {
        byte[] bytes = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(value, bytes, 0);
        return new BytesRef(bytes);
    }

    private static final FieldType INDEX_TERMS_FIELD_TYPE;
    static {
        INDEX_TERMS_FIELD_TYPE = new FieldType();
        INDEX_TERMS_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        INDEX_TERMS_FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
        INDEX_TERMS_FIELD_TYPE.setOmitNorms(true);
        INDEX_TERMS_FIELD_TYPE.setTokenized(false);
        INDEX_TERMS_FIELD_TYPE.freeze();
    }

    /** Mirrors {@code NumberFieldMapper.IndexTermsIntegerField}: sortable-bytes term plus numeric doc value. */
    static final class IndexTermsIntegerField extends Field {
        private final int numericVal;

        IndexTermsIntegerField(String name, int value) {
            super(name, encodeIndexTerm(value), INDEX_TERMS_FIELD_TYPE);
            this.numericVal = value;
        }

        @Override
        public Number numericValue() {
            return numericVal;
        }
    }

    /** The indexing/query-construction strategies being compared. */
    public enum Strategy {
        /** Today's default for an unformatted {@code integer} field: BKD points plus doc values. */
        POINT {
            @Override
            void addField(Document doc, int value) {
                doc.add(new IntField(FIELD, value, Field.Store.NO));
            }

            @Override
            Query termsQuery(int[] values) {
                return IntPoint.newSetQuery(FIELD, values);
            }
        },
        /** The {@code index_terms}-mapped path: sortable-bytes terms dictionary entries, doc values kept. */
        INDEX_TERMS {
            @Override
            void addField(Document doc, int value) {
                doc.add(new IndexTermsIntegerField(FIELD, value));
            }

            @Override
            Query termsQuery(int[] values) {
                List<BytesRef> terms = new ArrayList<>(values.length);
                for (int value : values) {
                    terms.add(encodeIndexTerm(value));
                }
                return new TermInSetQuery(FIELD, terms);
            }
        };

        abstract void addField(Document doc, int value);

        abstract Query termsQuery(int[] values);
    }

    @Param({ "POINT", "INDEX_TERMS" })
    public Strategy strategy;

    @Param({ "1", "10", "100", "1000", "10000", "100000" })
    public int nTerms;

    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private int[] queryValues;
    private Query prebuiltQuery;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        // @Fork(1) restarts the JVM per (strategy, nTerms) combination, so nothing in memory
        // survives between trials. Persist the index on disk instead, keyed by strategy and doc
        // count, so only the first trial for a given strategy pays to build and force-merge it;
        // the other nTerms trials for that strategy just open the existing directory.
        Path path = Path.of(System.getProperty("tests.index"), strategy.name() + "-" + N_DOCS);
        directory = FSDirectory.open(path);
        if (DirectoryReader.indexExists(directory) == false) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
                for (int docId = 0; docId < N_DOCS; docId++) {
                    Document doc = new Document();
                    strategy.addField(doc, docId);
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
        }
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);

        // Fixed seed: POINT and INDEX_TERMS search the exact same values at a given nTerms.
        queryValues = distinctRandomInts(new Random(42), nTerms, N_DOCS);

        prebuiltQuery = strategy.termsQuery(queryValues);
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
    public Query buildQuery() {
        return strategy.termsQuery(queryValues);
    }

    @Benchmark
    public TopDocs search() throws IOException {
        return searcher.search(prebuiltQuery, TOP_N);
    }
}
