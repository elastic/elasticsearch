/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.ColumnarStringTermQuery;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.DictionaryStringColumnReader;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.index.codec.Elasticsearch96Codec;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * What a ColumNAR keyword column's layout is worth to exact equality, measured on the generational state
 * each build reaches on its own. The method names describe the path a dictionary column takes: a term the
 * dictionary names, a term that escaped it, and a term the column does not hold. A plain column answers all
 * three by scanning, which is the comparison.
 *
 * <p>{@code minCoverage} is a parameter because the two bars answer different questions on the 92%
 * recurring workload. At 0.5 both builds reach a dictionary, so the arms compare two dictionaries whose
 * composition differs. At 0.9 only this branch reaches one, so they compare the two layouts. Neither bar is
 * this benchmark's claim about what ships; 0.9 is the policy in #160549.
 *
 * <p>Probe terms come from the values written rather than from either build's dictionary, so both builds
 * answer identical queries whatever layout each settled on. The layout reached, and where each probe
 * landed in it, is reported per trial on the {@code EQSTATE} line.
 *
 * <p>The query is the one production builds. {@code KeywordFieldType.binaryQueries} asks
 * {@code BinaryDocValuesQueries.forFormat}, which answers with {@code ColumnarBinaryDocValuesQueries} for a
 * {@code COLUMNAR_PAYLOAD} field, and that calls {@code ColumnarStringTermQuery.term}. The one difference
 * is the {@link org.elasticsearch.columnar.ScanBudget}: production passes the decode circuit breaker and
 * this passes a no-op, since there is no breaker to charge here.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class ColumnarStringEqualityBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    private static final String FIELD = "message";
    private static final int SEGMENTS_PER_GENERATION = 4;
    private static final int POOL_TERMS = 6_000;
    private static final int POOL_DRAW_PERCENT = 30;
    private static final int RECURRING_PERCENT = 92;
    private static final String POOL_PREFIX = "/api/v2/checkout/session/pool-";
    private static final String ONE_OFF_PREFIX = "/api/v2/checkout/session/one-off-";
    private static final String ABSENT_TERM = "/api/v2/checkout/session/definitely-absent-term";
    private static final int DICTIONARY_MAX_BYTES = 512 * 1024;
    private static final double MAX_SHARE_OF_COLUMN = 0.2;
    private static final int SEED = 11;

    @Param({ "4", "6", "8" })
    private int generation;

    @Param({ "0.5", "0.9" })
    private double minCoverage;

    private DictionaryPolicy policy;

    private Path path;
    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private Query dictionaryHit;
    private Query escapedHit;
    private Query miss;

    @Setup(Level.Trial)
    public void buildGenerationalState() throws IOException {
        path = Files.createTempDirectory("columnar-equality");
        directory = new MMapDirectory(path);
        policy = new DictionaryPolicy(DICTIONARY_MAX_BYTES, minCoverage, MAX_SHARE_OF_COLUMN);
        final Random random = new Random(SEED);
        int document = 0;
        for (int g = 0; g < generation; g++) {
            document = flush(random, document);
            forceMerge();
        }
        reader = DirectoryReader.open(directory);
        if (reader.leaves().size() != 1) {
            throw new AssertionError("expected one segment, got " + reader.leaves().size());
        }
        searcher = new IndexSearcher(reader);
        // NOTE: the default LRUQueryCache answers every repeat of the same query from a cached bitset, which
        // measures the cache rather than the column and makes both layouts look identical.
        searcher.setQueryCache(null);

        final StringColumnReader column = stringColumn(reader.leaves().get(0).reader());
        final Map<String, Integer> frequency = frequencies();
        final String named = medianByFrequency(frequency, POOL_PREFIX);
        final String escaped = medianByFrequency(frequency, ONE_OFF_PREFIX);
        if (frequency.containsKey(ABSENT_TERM)) {
            throw new AssertionError("the absent term is present");
        }
        dictionaryHit = ColumnarStringTermQuery.term(FIELD, new BytesRef(named), s -> {});
        escapedHit = ColumnarStringTermQuery.term(FIELD, new BytesRef(escaped), s -> {});
        miss = ColumnarStringTermQuery.term(FIELD, new BytesRef(ABSENT_TERM), s -> {});
        final Set<String> inDictionary = dictionaryTerms(column);
        // NOTE: a plain column names nothing, so this holds only where a dictionary was reached. Without it a
        // change in term selection could move a probe to the other path and leave the method names lying.
        if (column.hasDictionary()) {
            if (inDictionary.contains(named) == false) {
                throw new AssertionError("the dictionary probe escaped the dictionary: " + named);
            }
            if (inDictionary.contains(escaped)) {
                throw new AssertionError("the escape probe is in the dictionary: " + escaped);
            }
        }

        // NOTE: the benchmarks log4j config pins the root level to error, so trial state has to be printed
        // rather than logged, as the sibling benchmarks in this package do.
        System.out.println(
            "EQSTATE generation="
                + generation
                + " minCoverage="
                + minCoverage
                + " values="
                + column.numValues()
                + " layout="
                + (column.hasDictionary() ? "DICTIONARY" : "PLAIN")
                + " dictionaryTerms="
                + (column.hasDictionary() ? column.dictionarySize() : 0)
                + " escapes="
                + column.escapeCount()
                + " namedTerm="
                + named
                + " namedFrequency="
                + frequency.get(named)
                + " namedMatches="
                + searcher.count(dictionaryHit)
                + " namedInDictionary="
                + inDictionary.contains(named)
                + " escapedTerm="
                + escaped
                + " escapedFrequency="
                + frequency.get(escaped)
                + " escapedMatches="
                + searcher.count(escapedHit)
                + " escapedInDictionary="
                + inDictionary.contains(escaped)
                + " missMatches="
                + searcher.count(miss)
        );
    }

    @Benchmark
    public void dictionaryTermHit(Blackhole bh) throws IOException {
        bh.consume(searcher.count(dictionaryHit));
    }

    @Benchmark
    public void escapedTermHit(Blackhole bh) throws IOException {
        bh.consume(searcher.count(escapedHit));
    }

    @Benchmark
    public void absentTermMiss(Blackhole bh) throws IOException {
        bh.consume(searcher.count(miss));
    }

    /**
     * The median term by frequency of one population of the workload. Chosen from the values written rather
     * than from either build's dictionary, so the two builds answer the same query and the scores compare.
     */
    private static String medianByFrequency(Map<String, Integer> frequency, String prefix) {
        final List<Map.Entry<String, Integer>> candidates = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : frequency.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                candidates.add(entry);
            }
        }
        if (candidates.isEmpty()) {
            throw new AssertionError("no term under " + prefix);
        }
        candidates.sort(Map.Entry.<String, Integer>comparingByValue().thenComparing(Map.Entry::getKey));
        return candidates.get(candidates.size() / 2).getKey();
    }

    private static Set<String> dictionaryTerms(StringColumnReader column) throws IOException {
        final Set<String> terms = new HashSet<>();
        if (column.hasDictionary() == false) {
            return terms;
        }
        final DictionaryStringColumnReader dictionary = (DictionaryStringColumnReader) column;
        final BytesRef scratch = new BytesRef();
        for (int t = 0; t < dictionary.dictionarySize(); t++) {
            dictionary.termAt(org.elasticsearch.columnar.string.StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL + t, scratch);
            terms.add(scratch.utf8ToString());
        }
        return terms;
    }

    /**
     * One generation of the workload: {@link #SEGMENTS_PER_GENERATION} segments, each drawing a share of a
     * fixed pool and padding it with values unique to the index. The index and the frequency table are both
     * built through this, so neither can come to describe a workload the other did not see.
     *
     * @param document the running count of one-off values, which names them and so must carry across calls
     * @return that count after this generation
     */
    private static int generate(Random random, int document, WorkloadSink sink) throws IOException {
        for (int segment = 0; segment < SEGMENTS_PER_GENERATION; segment++) {
            final List<String> values = new ArrayList<>();
            for (int term = 0; term < POOL_TERMS; term++) {
                if (random.nextInt(100) >= POOL_DRAW_PERCENT) {
                    continue;
                }
                values.add(POOL_PREFIX + Integer.toString(term, 36));
            }
            final int tail = values.size() * (100 - RECURRING_PERCENT) / RECURRING_PERCENT;
            for (int i = 0; i < tail; i++, document++) {
                values.add(ONE_OFF_PREFIX + Integer.toString(document, 36));
            }
            Collections.shuffle(values, random);
            for (String value : values) {
                sink.value(value);
            }
            sink.endOfSegment();
        }
        return document;
    }

    private interface WorkloadSink {
        void value(String value) throws IOException;

        void endOfSegment() throws IOException;
    }

    /** The true frequency of every value, replayed from the same seed rather than read back off the column. */
    private Map<String, Integer> frequencies() throws IOException {
        final Map<String, Integer> frequency = new HashMap<>();
        final Random random = new Random(SEED);
        int document = 0;
        for (int g = 0; g < generation; g++) {
            document = generate(random, document, new WorkloadSink() {
                @Override
                public void value(String value) {
                    frequency.merge(value, 1, Integer::sum);
                }

                @Override
                public void endOfSegment() {}
            });
        }
        return frequency;
    }

    private int flush(Random random, int document) throws IOException {
        try (IndexWriter writer = new IndexWriter(directory, config(NoMergePolicy.INSTANCE, policy))) {
            return generate(random, document, new WorkloadSink() {
                @Override
                public void value(String value) throws IOException {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, payload(value), FIELD_TYPE));
                    writer.addDocument(doc);
                }

                @Override
                public void endOfSegment() throws IOException {
                    writer.commit();
                }
            });
        }
    }

    private void forceMerge() throws IOException {
        final LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
        mergePolicy.setMergeFactor(SEGMENTS_PER_GENERATION + 1);
        mergePolicy.setNoCFSRatio(0.0);
        try (IndexWriter writer = new IndexWriter(directory, config(mergePolicy, policy))) {
            writer.forceMerge(1);
        }
    }

    private static final FieldType FIELD_TYPE = binaryFieldType();

    private static FieldType binaryFieldType() {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.freeze();
        return type;
    }

    private static IndexWriterConfig config(MergePolicy mergePolicy, DictionaryPolicy dictionaryPolicy) {
        final DocValuesFormat format = new ColumNARDocValuesFormat(
            (fieldName, fieldType) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            dictionaryPolicy,
            StringColumnOptions.DEFAULT_SUMMARY
        );
        final Codec codec = new Elasticsearch96Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return format;
            }
        };
        return new IndexWriterConfig().setCodec(codec).setUseCompoundFile(false).setMergePolicy(mergePolicy);
    }

    private static BytesRef payload(String value) {
        final List<BytesRef> slots = new ArrayList<>(1);
        slots.add(new BytesRef(value));
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(slots));
    }

    private static StringColumnReader stringColumn(LeafReader leaf) throws IOException {
        final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
        if ((values instanceof ColumnarStringBinaryDocValues) == false) {
            throw new AssertionError("expected a columnar column, got " + values);
        }
        return ((ColumnarStringBinaryDocValues) values).reader();
    }

    @TearDown(Level.Trial)
    public void close() throws IOException {
        reader.close();
        directory.close();
        try (Stream<Path> files = Files.walk(path)) {
            files.sorted(Comparator.reverseOrder()).forEach(file -> {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
        }
    }
}
