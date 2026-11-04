/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.querydsl.query.MatchPhraseQuery;
import org.elasticsearch.xpack.esql.querydsl.query.MatchQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * BM25 scores of runtime text must equal, bit for bit, the scores of a single-segment Lucene index of the same rows
 * with the same analyzer and the default {@code text} similarity (Goal 3 of the design).
 * <p>
 * The statistics come from {@link RuntimeBm25RowStats} added up per position, as the statistics pass adds them with
 * {@code SUM(MV_SLICE(...))}. The scores come from {@link RuntimeSearch#bm25ScoreEvaluator}. The reference index
 * holds the real {@code totalTermFreq} and {@code sumDocFreq}, so equal scores also show that the placeholders the
 * statistics searcher returns for them ({@code df} and {@code N}) do not change a BM25 score.
 */
public class RuntimeBm25ParityTests extends AbstractRuntimeSearchEvaluatorTests {

    private static final Expression FIELD = EsqlTestUtils.referenceAttribute("content", DataType.TEXT);
    private static final String FIELD_SOURCE = "content";
    private static final String SYNONYMS = "test_synonyms";
    private static final List<String> REGISTRY_ANALYZERS = List.of("standard", "simple", "whitespace", "stop");
    private static final List<String> VOCABULARY = List.of(
        "quick",
        "brown",
        "fox",
        "jumps",
        "over",
        "lazy",
        "dog",
        "Battery",
        "drains",
        "overnight",
        "car",
        "auto",
        "red",
        "the",
        "a",
        "and",
        "of"
    );

    /** Whether an option key keeps parity with {@code bm25}, following the option table in the Technical Plan. */
    private enum Support {
        SUPPORTED,
        REJECTED_WITH_BM25
    }

    private static final Map<String, Support> OPTION_TABLE = Map.ofEntries(
        entry("operator", Support.SUPPORTED),
        entry("minimum_should_match", Support.SUPPORTED),
        entry("boost", Support.SUPPORTED),
        entry("zero_terms_query", Support.SUPPORTED),
        entry("lenient", Support.SUPPORTED),
        entry("auto_generate_synonyms_phrase_query", Support.SUPPORTED),
        entry("slop", Support.SUPPORTED),
        entry("analyzer", Support.SUPPORTED),
        entry("fuzziness", Support.REJECTED_WITH_BM25),
        entry("prefix_length", Support.REJECTED_WITH_BM25),
        entry("max_expansions", Support.REJECTED_WITH_BM25),
        entry("fuzzy_transpositions", Support.REJECTED_WITH_BM25),
        entry("fuzzy_rewrite", Support.REJECTED_WITH_BM25)
    );

    public void testOptionTableCoversEveryKey() {
        Set<String> keys = new HashSet<>(Match.ALLOWED_OPTIONS.keySet());
        keys.addAll(MatchPhrase.ALLOWED_OPTIONS.keySet());
        assertThat(OPTION_TABLE.keySet(), equalTo(keys));
    }

    public void testMatchParity() throws IOException {
        int scoredRows = 0;
        for (int iteration = 0; iteration < 10; iteration++) {
            String analyzer = randomFrom(REGISTRY_ANALYZERS);
            Map<String, Object> options = randomOptions(Match.ALLOWED_OPTIONS.keySet(), analyzer);
            scoredRows += assertParity(
                analyzer,
                null,
                randomCorpus(),
                new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, randomQuery(), options)
            );
        }
        // Guards against a vacuous pass where both sides score every row 0.
        assertThat(scoredRows, greaterThan(0));
    }

    public void testMatchPhraseParity() throws IOException {
        int scoredRows = 0;
        for (int iteration = 0; iteration < 10; iteration++) {
            String analyzer = randomFrom(REGISTRY_ANALYZERS);
            Map<String, Object> options = randomOptions(MatchPhrase.ALLOWED_OPTIONS.keySet(), analyzer);
            String query = randomQuery();
            List<List<String>> corpus = new ArrayList<>(randomCorpus());
            // Random phrases rarely occur in random rows, so add one row that holds the phrase.
            corpus.add(List.of(query));
            scoredRows += assertParity(
                analyzer,
                null,
                corpus,
                new MatchPhraseQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, query, options)
            );
        }
        // Guards against a vacuous pass where both sides score every row 0.
        assertThat(scoredRows, greaterThan(0));
    }

    /** One analyzer with synonyms analyzes both the rows and the query, as the design requires with {@code bm25}. */
    public void testSynonymParity() throws IOException {
        Analyzer synonyms = RuntimeBm25RowStatsTests.synonymAnalyzer();
        for (int iteration = 0; iteration < 5; iteration++) {
            String query = randomFrom("car", "auto", "red car", "auto drains", "car auto");
            List<List<String>> corpus = new ArrayList<>(randomCorpus());
            // Make sure each run has rows the synonym query matches.
            corpus.add(List.of("red car"));
            corpus.add(List.of("an auto that drains"));
            int scored = assertParity(
                SYNONYMS,
                synonyms,
                corpus,
                new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, query, Map.of())
            );
            assertThat(scored, greaterThan(0));
        }
    }

    /** A query that analyzes to nothing becomes match-all with {@code zero_terms_query: all}, and matches every row. */
    public void testZeroTermsQueryAll() throws IOException {
        assertParity(
            "stop",
            null,
            randomCorpus(),
            new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, "the and", Map.of("zero_terms_query", "all"))
        );
    }

    /**
     * Rows added between the passes can hold a term the statistics pass never saw. Its df is clamped to 1, the row
     * still gets a finite score, and the query gets the drift warning.
     */
    public void testClampWhenRowsChangedBetweenPasses() {
        List<List<String>> statisticsRows = List.of(List.of("quick brown fox"), List.of("brown fox"));
        List<List<String>> scoringRows = List.of(List.of("quick brown fox"), List.of("brown fox"), List.of("lazy dog"));
        NamedAnalyzer analyzer = registryAnalyzer("standard");
        Query query = compile(new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, "fox dog", Map.of()), analyzer);
        List<BytesRef> terms = RuntimeSearch.collectTerms(query);

        Scores scores = bm25Scores(analyzer, query, terms, boxed(statistics("standard", null, terms, statisticsRows)), scoringRows);

        assertThat(scores.values[2], greaterThan(0.0));
        assertTrue(Double.isFinite(scores.values[2]));
        assertDriftWarning(scores.warnings);
    }

    /** No row of the statistics pass had a token, so N = 0: matching rows score 0 and the query gets the warning. */
    public void testEmptyStatisticsWhenNIsZero() {
        NamedAnalyzer analyzer = registryAnalyzer("standard");
        Query query = compile(new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, "fox", Map.of()), analyzer);
        List<BytesRef> terms = RuntimeSearch.collectTerms(query);

        Scores scores = bm25Scores(analyzer, query, terms, List.of(0L, 0L, 0L), List.of(List.of("fox"), List.of("dog")));

        assertThat(Arrays.stream(scores.values).boxed().toList(), everyItem(equalTo(0.0)));
        assertDriftWarning(scores.warnings);
    }

    /** Null statistics (no row seen, or an overflowing sum) behave like N = 0. */
    public void testEmptyStatisticsWhenNull() {
        NamedAnalyzer analyzer = registryAnalyzer("standard");
        Query query = compile(new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, "fox", Map.of()), analyzer);
        List<BytesRef> terms = RuntimeSearch.collectTerms(query);
        List<Long> statistics = Arrays.asList(null, null, null);

        Scores scores = bm25Scores(analyzer, query, terms, statistics, List.of(List.of("fox")));

        assertThat(scores.values[0], equalTo(0.0));
        assertDriftWarning(scores.warnings);
    }

    /** A summed length below N can only come from data that changed between the passes; it is clamped to N. */
    public void testSummedLengthBelowNIsClamped() {
        NamedAnalyzer analyzer = registryAnalyzer("standard");
        Query query = compile(new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, "fox", Map.of()), analyzer);
        List<BytesRef> terms = RuntimeSearch.collectTerms(query);

        Scores scores = bm25Scores(analyzer, query, terms, List.of(5L, 2L, 1L), List.of(List.of("fox")));

        assertThat(scores.values[0], greaterThan(0.0));
        assertTrue(Double.isFinite(scores.values[0]));
        assertDriftWarning(scores.warnings);
    }

    /** A query that analyzes to nothing, with the default {@code zero_terms_query}, matches no row. */
    public void testMatchNoDocsScoresZero() {
        NamedAnalyzer analyzer = registryAnalyzer("stop");
        Query query = compile(new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, "the and", Map.of()), analyzer);
        List<BytesRef> terms = RuntimeSearch.collectTerms(query);

        List<List<String>> rows = new ArrayList<>();
        rows.add(List.of("fox"));
        rows.add(null);

        Scores scores = bm25Scores(analyzer, query, terms, List.of(2L, 4L), rows);

        assertThat(Arrays.stream(scores.values).boxed().toList(), everyItem(equalTo(0.0)));
        assertThat(scores.warnings, empty());
    }

    /** An empty first page does not fix the statistics: they are read from the first page that has rows. */
    public void testStatisticsReadFromFirstNonEmptyPage() {
        NamedAnalyzer analyzer = registryAnalyzer("standard");
        Query query = compile(new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, "fox", Map.of()), analyzer);
        List<BytesRef> terms = RuntimeSearch.collectTerms(query);
        List<List<String>> rows = List.of(List.of("fox"), List.of("quick fox"), List.of("dog"));
        List<Long> statistics = boxed(statistics("standard", null, terms, rows));
        double[] expected = bm25Scores(analyzer, query, terms, statistics, rows).values;

        ExpressionEvaluator.Factory factory = RuntimeSearch.bm25ScoreEvaluator(
            Source.EMPTY,
            "MATCH",
            toEvaluator().apply(FIELD),
            FIELD_SOURCE,
            query,
            analyzer,
            terms,
            constantLongs(statistics)
        );
        DriverContext context = driverContext();
        try (ExpressionEvaluator evaluator = factory.get(context)) {
            Page empty = new Page(corpusBlock(context, List.of()));
            try (DoubleBlock scores = (DoubleBlock) evaluator.eval(empty)) {
                assertThat(scores.getPositionCount(), equalTo(0));
            } finally {
                empty.releaseBlocks();
            }
            Page page = new Page(corpusBlock(context, rows));
            try (DoubleBlock scores = (DoubleBlock) evaluator.eval(page)) {
                for (int p = 0; p < rows.size(); p++) {
                    assertThat(scores.getDouble(scores.getFirstValueIndex(p)), equalTo(expected[p]));
                }
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /** Terms a fuzzy query expands to are unknown to the statistics pass, so the scorer refuses the query. */
    public void testTermExpandingQueryIsRejected() {
        NamedAnalyzer analyzer = registryAnalyzer("standard");
        Query query = compile(new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, "foxes", Map.of("fuzziness", "1")), analyzer);
        List<BytesRef> terms = RuntimeSearch.collectTerms(query);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RuntimeSearch.bm25ScoreEvaluator(
                Source.EMPTY,
                "MATCH",
                toEvaluator().apply(FIELD),
                FIELD_SOURCE,
                query,
                analyzer,
                terms,
                constantLongs(Arrays.asList(new Long[terms.size() + 2]))
            )
        );
        assertThat(e.getMessage(), containsString("fuzzy and other term-expanding queries are not supported"));
    }

    /** Rows that do not match score 0 and add no warning, even when the statistics are empty. */
    public void testNonMatchingRowsAddNoWarning() {
        NamedAnalyzer analyzer = registryAnalyzer("standard");
        Query query = compile(new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, "fox", Map.of()), analyzer);
        List<BytesRef> terms = RuntimeSearch.collectTerms(query);

        Scores scores = bm25Scores(analyzer, query, terms, List.of(0L, 0L, 0L), List.of(List.of("dog")));

        assertThat(scores.values[0], equalTo(0.0));
        assertThat(scores.warnings, empty());
    }

    /**
     * The data node analyzes the query again. If its terms differ from the terms the statistics were computed for,
     * the scorer refuses to build instead of scoring with missing statistics.
     */
    public void testAnalyzerMismatchThrows() {
        NamedAnalyzer standard = registryAnalyzer("standard");
        NamedAnalyzer whitespace = registryAnalyzer("whitespace");
        String text = "Quick Fox";
        List<BytesRef> shippedTerms = RuntimeSearch.collectTerms(
            compile(new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, text, Map.of()), whitespace)
        );
        Query nodeQuery = compile(new MatchQuery(Source.EMPTY, RuntimeSearch.CONTENT_FIELD, text, Map.of()), standard);

        Bm25AnalyzerMismatchException e = expectThrows(
            Bm25AnalyzerMismatchException.class,
            () -> RuntimeSearch.bm25ScoreEvaluator(
                Source.EMPTY,
                "MATCH",
                toEvaluator().apply(FIELD),
                FIELD_SOURCE,
                nodeQuery,
                standard,
                shippedTerms,
                constantLongs(List.of(1L, 2L, 1L, 1L))
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "[MATCH] cannot score [content] with BM25: the query analyzes to [fox, quick] on this node, but the statistics "
                    + "were computed for [Fox, Quick]; nodes may run different analysis plugins or versions"
            )
        );
    }

    /** Asserts parity and returns the number of rows that scored above 0. */
    private int assertParity(
        String analyzerName,
        Analyzer customAnalyzer,
        List<List<String>> corpus,
        org.elasticsearch.xpack.esql.core.querydsl.query.Query esqlQuery
    ) throws IOException {
        NamedAnalyzer analyzer = customAnalyzer == null
            ? registryAnalyzer(analyzerName)
            : new NamedAnalyzer(analyzerName, AnalyzerScope.GLOBAL, customAnalyzer);
        Query query = compile(esqlQuery, analyzer);
        List<BytesRef> terms = RuntimeSearch.collectTerms(query);

        float[] expected = referenceScores(analyzer, corpus, query);
        long[] statistics = statistics(analyzerName, customAnalyzer, terms, corpus);
        Scores actual = bm25Scores(analyzer, query, terms, boxed(statistics), corpus);

        String context = "query " + query + " with " + analyzerName + " over " + corpus;
        for (int row = 0; row < corpus.size(); row++) {
            assertThat(context + ", row " + row, actual.values[row], equalTo((double) expected[row]));
        }
        assertThat(context, actual.warnings, empty());
        return (int) Arrays.stream(actual.values).filter(score -> score > 0).count();
    }

    /** Scores every row with a real single-segment index, the reference of Goal 3. */
    private static float[] referenceScores(NamedAnalyzer analyzer, List<List<String>> corpus, Query query) throws IOException {
        NamedAnalyzer withGap = new NamedAnalyzer(analyzer, TextFieldMapper.Defaults.POSITION_INCREMENT_GAP);
        IndexWriterConfig config = new IndexWriterConfig(withGap).setSimilarity(SuppliedStatisticsSearcher.SIMILARITY)
            // LogDocMergePolicy merges adjacent segments only, so document ids stay equal to row numbers.
            .setMergePolicy(new LogDocMergePolicy());
        try (Directory directory = new ByteBuffersDirectory(); IndexWriter writer = new IndexWriter(directory, config)) {
            for (List<String> row : corpus) {
                Document document = new Document();
                if (row != null) {
                    for (String value : row) {
                        document.add(new TextField(RuntimeSearch.CONTENT_FIELD, value, Field.Store.NO));
                    }
                }
                writer.addDocument(document);
            }
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setSimilarity(SuppliedStatisticsSearcher.SIMILARITY);
                float[] scores = new float[corpus.size()];
                for (ScoreDoc hit : searcher.search(query, corpus.size()).scoreDocs) {
                    scores[hit.doc] = hit.score;
                }
                return scores;
            }
        }
    }

    /** Runs the statistics pass: {@link RuntimeBm25RowStats} per row, added up per position. */
    private long[] statistics(String analyzerName, Analyzer customAnalyzer, List<BytesRef> terms, List<List<String>> corpus) {
        RuntimeBm25RowStats rowStats = new RuntimeBm25RowStats(
            Source.EMPTY,
            FIELD,
            Literal.keyword(Source.EMPTY, analyzerName),
            RuntimeBm25RowStats.termsLiteral(Source.EMPTY, terms)
        );
        Map<String, Analyzer> custom = customAnalyzer == null ? Map.of() : Map.of(analyzerName, customAnalyzer);
        DriverContext context = driverContext();
        Page page = new Page(corpusBlock(context, corpus));
        long[] sums = new long[terms.size() + 2];
        try (
            ExpressionEvaluator evaluator = rowStats.toEvaluator(RuntimeBm25RowStatsTests.toEvaluatorWith(custom)).get(context);
            LongBlock rows = (LongBlock) evaluator.eval(page)
        ) {
            for (int p = 0; p < rows.getPositionCount(); p++) {
                if (rows.isNull(p)) {
                    continue;
                }
                int first = rows.getFirstValueIndex(p);
                assertThat(rows.getValueCount(p), equalTo(sums.length));
                for (int i = 0; i < sums.length; i++) {
                    sums[i] += rows.getLong(first + i);
                }
            }
        } finally {
            page.releaseBlocks();
        }
        return sums;
    }

    private record Scores(double[] values, List<String> warnings) {}

    private Scores bm25Scores(NamedAnalyzer analyzer, Query query, List<BytesRef> terms, List<Long> statistics, List<List<String>> corpus) {
        ExpressionEvaluator.Factory factory = RuntimeSearch.bm25ScoreEvaluator(
            Source.EMPTY,
            "MATCH",
            toEvaluator().apply(FIELD),
            FIELD_SOURCE,
            query,
            analyzer,
            terms,
            constantLongs(statistics)
        );
        DriverContext context = driverContext();
        Page page = new Page(corpusBlock(context, corpus));
        double[] values = new double[corpus.size()];
        try (ExpressionEvaluator evaluator = factory.get(context); DoubleBlock scores = (DoubleBlock) evaluator.eval(page)) {
            assertThat(scores.getPositionCount(), equalTo(corpus.size()));
            for (int p = 0; p < values.length; p++) {
                values[p] = scores.getDouble(scores.getFirstValueIndex(p));
            }
        } finally {
            page.releaseBlocks();
        }
        context.finish();
        return new Scores(values, context.warnings());
    }

    private static void assertDriftWarning(List<String> warnings) {
        assertThat(warnings, hasSize(1));
        assertThat(
            warnings.get(0),
            containsString(
                "the statistics and scoring passes read different data for [content]; BM25 scores use clamped or empty statistics"
            )
        );
    }

    private static Block corpusBlock(DriverContext context, List<List<String>> corpus) {
        try (BytesRefBlock.Builder builder = context.blockFactory().newBytesRefBlockBuilder(corpus.size())) {
            for (List<String> row : corpus) {
                if (row == null) {
                    builder.appendNull();
                } else if (row.size() == 1) {
                    builder.appendBytesRef(new BytesRef(row.get(0)));
                } else {
                    builder.beginPositionEntry();
                    for (String value : row) {
                        builder.appendBytesRef(new BytesRef(value));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private static List<ExpressionEvaluator.Factory> constantLongs(List<Long> values) {
        List<ExpressionEvaluator.Factory> factories = new ArrayList<>(values.size());
        for (Long value : values) {
            factories.add(context -> new ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    int positions = page.getPositionCount();
                    return value == null
                        ? context.blockFactory().newConstantNullBlock(positions)
                        : context.blockFactory().newConstantLongBlockWith(value, positions);
                }

                @Override
                public long baseRamBytesUsed() {
                    return 0;
                }

                @Override
                public void close() {}
            });
        }
        return factories;
    }

    private static List<Long> boxed(long[] values) {
        return Arrays.stream(values).boxed().toList();
    }

    private static NamedAnalyzer registryAnalyzer(String name) {
        return RuntimeSearch.resolveNamedAnalyzer(name, toEvaluator());
    }

    private static Query compile(org.elasticsearch.xpack.esql.core.querydsl.query.Query query, NamedAnalyzer analyzer) {
        return RuntimeSearch.compileQuery(query, analyzer);
    }

    /** Rows of one to three values, with some null rows and rows that may analyze to no tokens. */
    private static List<List<String>> randomCorpus() {
        int rows = between(1, 120);
        List<List<String>> corpus = new ArrayList<>(rows);
        for (int r = 0; r < rows; r++) {
            if (rarely()) {
                corpus.add(null);
                continue;
            }
            List<String> values = new ArrayList<>();
            int valueCount = randomBoolean() ? 1 : between(2, 3);
            for (int v = 0; v < valueCount; v++) {
                values.add(randomWords(between(1, 8)));
            }
            corpus.add(values);
        }
        return corpus;
    }

    private static String randomQuery() {
        return randomWords(between(1, 4));
    }

    private static String randomWords(int count) {
        List<String> words = new ArrayList<>(count);
        for (int w = 0; w < count; w++) {
            words.add(randomFrom(VOCABULARY));
        }
        return String.join(" ", words);
    }

    /** A random subset of the supported options that apply to the function, with values that keep parity. */
    private static Map<String, Object> randomOptions(Set<String> allowedKeys, String analyzer) {
        Map<String, Object> options = new HashMap<>();
        for (String key : allowedKeys) {
            Support support = OPTION_TABLE.get(key);
            assertNotNull("option [" + key + "] is missing from the option table", support);
            if (support == Support.REJECTED_WITH_BM25 || randomBoolean()) {
                continue;
            }
            options.put(key, switch (key) {
                case "operator" -> randomFrom("or", "and");
                case "minimum_should_match" -> randomFrom("1", "2", "50%", "-1");
                case "boost" -> (float) randomDoubleBetween(0.5, 3.0, true);
                case "zero_terms_query" -> randomFrom("none", "all");
                case "lenient", "auto_generate_synonyms_phrase_query" -> randomBoolean();
                case "slop" -> between(0, 3);
                // With bm25 the query analyzer must equal the values analyzer.
                case "analyzer" -> analyzer;
                default -> throw new AssertionError("no value generator for supported option [" + key + "]");
            });
        }
        return options;
    }
}
