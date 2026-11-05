/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.query.wildcard;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.benchmark.index.mapper.MapperServiceFactory;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.stats.SearchStatsSettings;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xpack.wildcard.Wildcard;
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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Measures what case-insensitive queries on a {@code wildcard} field cost, both to build and to run, against the same queries
 * run case-sensitively. A case-insensitive query looks each of its ngrams up under every form a case variant of it could have been
 * indexed under, so it can consult more terms than a case-sensitive one; this benchmark shows how much that costs on mostly-ASCII
 * text and on text mixing scripts whose case folding differs from lowercasing.
 * <p>
 * Setup checks that every query finds the same documents on the {@code wildcard} field as on a {@code keyword} field holding the
 * same values, so a configuration that gives wrong results is never timed.
 * <p>
 * Setup also prints, for each configuration, how many index terms the queries look up and how many documents they find on average.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@SuppressWarnings("unused") // invoked by JMH
public class WildcardCaseInsensitiveQueryBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    private static final int NUM_DOCS = 50_000;
    private static final int NUM_QUERIES = 100;

    /** Code point ranges whose letters make up the words of each data set, as {@code [first, last]} pairs. */
    public enum Dataset {
        /** Lowercase ASCII letters. */
        ASCII(new int[][] { { 'a', 'z' } }),
        /** ASCII plus Latin Extended-A (Turkish dotless i, long s, ...), Greek (final sigma, ...) and Cyrillic letters. */
        MIXED(new int[][] { { 'a', 'z' }, { 0x0100, 0x017F }, { 0x03B1, 0x03C9 }, { 0x0430, 0x044F } });

        private final int[][] ranges;

        Dataset(int[][] ranges) {
            this.ranges = ranges;
        }
    }

    public enum QueryType {
        TERM,
        PREFIX,
        WILDCARD,
        REGEXP
    }

    @Param
    public Dataset dataset;

    @Param
    public QueryType queryType;

    @Param({ "true", "false" })
    public boolean caseInsensitive;

    private Directory directory;
    private IndexReader reader;
    private IndexSearcher searcher;
    private MapperService mapperService;
    private SearchExecutionContext context;
    private List<String> queryTexts;
    private List<Query> queries;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        mapperService = MapperServiceFactory.create("""
            {"_doc":{"properties":{"wc":{"type":"wildcard"},"kw":{"type":"keyword"}}}}""", List.of(new Wildcard()));
        Random random = new Random(42);
        List<String> values = new ArrayList<>(NUM_DOCS);
        directory = new ByteBuffersDirectory();
        try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig(IndexShard.buildIndexAnalyzer(mapperService)))) {
            for (int i = 0; i < NUM_DOCS; i++) {
                String value = randomValue(random);
                values.add(value);
                String json = new String(JsonStringEncoder.getInstance().quoteAsUTF8(value), StandardCharsets.UTF_8);
                String source = "{\"wc\":\"" + json + "\",\"kw\":\"" + json + "\"}";
                iw.addDocuments(
                    mapperService.documentMapper()
                        .parse(new SourceToParse(Integer.toString(i), new BytesArray(source), XContentType.JSON))
                        .docs()
                );
            }
            iw.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        context = buildSearchExecutionContext();

        queryTexts = new ArrayList<>(NUM_QUERIES);
        for (int i = 0; i < NUM_QUERIES; i++) {
            queryTexts.add(queryText(values.get(random.nextInt(values.size())), random));
        }
        queries = buildQueries(mapperService.fieldType("wc"));
        selfTest();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        IOUtils.close(reader, directory);
    }

    /** The cost of turning query text into a query, which includes looking up the case variants of every ngram. */
    @Benchmark
    public List<Query> build() {
        return buildQueries(mapperService.fieldType("wc"));
    }

    /** The cost of running the queries, which includes looking up every form of every ngram in the index. */
    @Benchmark
    public long search() throws IOException {
        long hits = 0;
        for (Query query : queries) {
            hits += searcher.count(query);
        }
        return hits;
    }

    private List<Query> buildQueries(MappedFieldType fieldType) {
        List<Query> built = new ArrayList<>(queryTexts.size());
        for (String text : queryTexts) {
            built.add(buildQuery(fieldType, text));
        }
        return built;
    }

    private Query buildQuery(MappedFieldType fieldType, String text) {
        return switch (queryType) {
            case TERM -> caseInsensitive ? fieldType.termQueryCaseInsensitive(text, context) : fieldType.termQuery(text, context);
            case PREFIX -> fieldType.prefixQuery(text, null, caseInsensitive, context);
            case WILDCARD -> fieldType.wildcardQuery(text, null, caseInsensitive, context);
            case REGEXP -> fieldType.regexpQuery(
                text,
                RegExp.ALL,
                caseInsensitive ? RegExp.CASE_INSENSITIVE : 0,
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                null,
                context
            );
        };
    }

    private void selfTest() throws IOException {
        List<Query> keywordQueries = buildQueries(mapperService.fieldType("kw"));
        long totalTerms = 0;
        long totalHits = 0;
        for (int i = 0; i < queries.size(); i++) {
            int expected = searcher.count(keywordQueries.get(i));
            int actual = searcher.count(queries.get(i));
            if (expected != actual) {
                throw new AssertionError(
                    "[" + queryTexts.get(i) + "] found [" + actual + "] documents on the wildcard field but [" + expected + "] on keyword"
                );
            }
            totalTerms += countTerms(queries.get(i));
            totalHits += actual;
        }
        System.out.printf(
            Locale.ROOT,
            "[%s/%s/case_insensitive=%s] index terms per query [%.2f], hits per query [%.2f]%n",
            dataset,
            queryType,
            caseInsensitive,
            (double) totalTerms / queries.size(),
            (double) totalHits / queries.size()
        );
    }

    private static long countTerms(Query query) {
        long[] count = new long[1];
        query.visit(new QueryVisitor() {
            @Override
            public void consumeTerms(Query q, Term... terms) {
                count[0] += terms.length;
            }

            @Override
            public void consumeTermsMatching(Query q, String field, Supplier<ByteRunAutomaton> automaton) {
                if (q instanceof TermInSetQuery termInSet) {
                    count[0] += termInSet.getTermsCount();
                }
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                return this;
            }
        });
        return count[0];
    }

    /**
     * Query text taken from an indexed value. Case-insensitive queries change the case of some words, so they only match by
     * ignoring case; case-sensitive ones keep it, so both kinds of query find documents.
     */
    private String queryText(String value, Random random) {
        String text = caseInsensitive ? changeCase(value, random) : value;
        int[] codePoints = text.codePoints().toArray();
        return switch (queryType) {
            case TERM -> text;
            case PREFIX -> new String(codePoints, 0, Math.max(3, codePoints.length / 2));
            case WILDCARD -> "*" + middle(codePoints, random) + "*";
            case REGEXP -> ".*" + middle(codePoints, random) + ".*";
        };
    }

    private static String middle(int[] codePoints, Random random) {
        int length = Math.min(codePoints.length, 6 + random.nextInt(7));
        int start = random.nextInt(codePoints.length - length + 1);
        return new String(codePoints, start, length);
    }

    private static String changeCase(String value, Random random) {
        StringBuilder changed = new StringBuilder();
        for (String word : value.split(" ")) {
            if (changed.isEmpty() == false) {
                changed.append(' ');
            }
            changed.append(random.nextBoolean() ? word.toUpperCase(Locale.ROOT) : word.toLowerCase(Locale.ROOT));
        }
        return changed.toString();
    }

    private String randomValue(Random random) {
        StringBuilder value = new StringBuilder();
        int words = 2 + random.nextInt(4);
        for (int w = 0; w < words; w++) {
            if (w > 0) {
                value.append(' ');
            }
            int[] range = dataset.ranges[random.nextInt(dataset.ranges.length)];
            StringBuilder word = new StringBuilder();
            int length = 3 + random.nextInt(8);
            while (word.length() < length) {
                int codePoint = range[0] + random.nextInt(range[1] - range[0] + 1);
                if (Character.isLetter(codePoint)) {
                    word.appendCodePoint(codePoint);
                }
            }
            int casing = random.nextInt(20);
            value.append(
                casing < 12 ? word
                    : casing < 17 ? word.substring(0, 1).toUpperCase(Locale.ROOT) + word.substring(1)
                    : word.toString().toUpperCase(Locale.ROOT)
            );
        }
        return value.toString();
    }

    private SearchExecutionContext buildSearchExecutionContext() {
        return new SearchExecutionContext(
            0,
            0,
            mapperService.getIndexSettings(),
            null,
            (ft, fdc) -> ft.fielddataBuilder(fdc).build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService()),
            mapperService,
            mapperService.mappingLookup(),
            new SimilarityService(mapperService.getIndexSettings(), null, Map.of()),
            null,
            XContentParserConfiguration.EMPTY.withRegistry(new NamedXContentRegistry(ClusterModule.getNamedXWriteables()))
                .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
            new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
            null,
            searcher,
            () -> 1L,
            null,
            null,
            () -> true,
            null,
            Collections.emptyMap(),
            null,
            MapperMetrics.NOOP,
            new ShardSearchStats(new SearchStatsSettings(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)))
        );
    }
}
