/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.lucene.query.LuceneQueryEvaluator;
import org.elasticsearch.compute.lucene.query.LuceneQueryExpressionEvaluator;
import org.elasticsearch.compute.lucene.query.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.search.stats.SearchStatsSettings;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeSearchTextEvaluator;
import org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeSearchTextWithLuceneQueryEvaluator;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.RuntimeSearchExecutionContext;
import org.elasticsearch.xpack.esql.querydsl.query.MatchPhraseQuery;
import org.elasticsearch.xpack.esql.querydsl.query.MatchQuery;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Compares ES|QL {@code MATCH} / {@code MATCH_PHRASE} with Query DSL {@code match} / {@code match_phrase}.
 *
 * <p>{@code esqlWhere}, {@code esqlEval}, {@code dslCount}, and {@code dslScored} scan one shared single-segment
 * index. {@code esqlRuntime} and {@code dslMemIndex} match {@value #BLOCK_LENGTH} in-memory rows with no inverted
 * index; don't compare those two groups. There is no {@code @OperationsPerInvocation} because they process different
 * document counts.
 *
 * <p>{@code dslCount} and {@code dslScored} compile a {@link Weight} in {@link #setup()}; the ES|QL mapped paths
 * rebuild a single-pass scanner per op, so their per-doc numbers run a bit high. {@link #setup()} checks hit counts
 * twice before JMH runs.
 */
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 7, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class MatchVsDslBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    private static final int BLOCK_LENGTH = 128;
    /** Heap size for {@code dslScored}. */
    private static final int TOP_K = 10;
    private static final int MAX_PAGE_SIZE = BLOCK_LENGTH;
    private static final String FIELD = "content";
    private static final String[] PHRASE = { "quick", "brown", "fox" };
    private static final String PHRASE_TEXT = "quick brown fox";
    /** Fuzzy of the planted "brown", so {@code matchFuzzy} hits every row. */
    private static final String FUZZY_TEXT = "bron";
    private static final Set<String> PHRASE_TERMS = Set.of(PHRASE);

    /** Keeps the shared index to a few million tokens, with enough docs that createWeight isn't the whole op. */
    private static final int INDEX_TOKEN_BUDGET = 4_000_000;
    private static final int MIN_INDEX_WINDOWS = 8;
    private static final int MAX_INDEX_WINDOWS = 64;

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    private static final FoldContext FOLD_CONTEXT = FoldContext.small();

    /** Number of tokens in each row. */
    @Param({ "16", "128", "1024", "8192" })
    public int textTokens;

    /** Engine under test. */
    @Param({ "esqlWhere", "esqlEval", "dslCount", "dslScored", "esqlRuntime", "dslMemIndex" })
    public String engine;

    /** Query shape. */
    @Param({ "phrase", "phraseMiss", "phraseSlop", "match", "matchAnd", "matchFuzzy" })
    public String query;

    private String[] contents;
    private NamedAnalyzer analyzer;
    private int windows;
    private int indexDocs;

    // esqlRuntime
    private Page runtimePage;
    private ExpressionEvaluator runtimeEvaluator;

    // esqlEval
    private Page[] docPages;
    private ExpressionEvaluator.Factory evalFactory;

    // esqlWhere
    private IndexedByShardId<BenchmarkShardContext> whereShardContexts;

    // Shared index + Query DSL
    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private Query luceneQuery;
    private Weight countWeight; // dslCount, compiled in setup()
    private Weight scoreWeight; // dslScored, compiled in setup()
    private MemoryIndex memoryIndex;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Random random = new Random(42L);
        contents = new String[BLOCK_LENGTH];
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            contents[i] = row(random);
        }

        analyzer = new NamedAnalyzer(
            "standard",
            AnalyzerScope.GLOBAL,
            new StandardAnalyzer(),
            TextFieldMapper.Defaults.POSITION_INCREMENT_GAP
        );

        windows = indexWindows();
        indexDocs = windows * BLOCK_LENGTH;

        validateQueryShape();

        switch (engine) {
            case "esqlRuntime" -> {
                runtimePage = buildRuntimePage();
                runtimeEvaluator = buildRuntimeEvaluator();
            }
            case "esqlEval" -> {
                buildIndex(indexDocs);
                // Rewrite once; a fuzzy rewrite can dwarf the scan we're measuring.
                luceneQuery = searcher.rewrite(buildLuceneQuery());
                evalFactory = new LuceneQueryExpressionEvaluator.Factory(
                    new IndexedByShardIdFromSingleton<>(new LuceneQueryEvaluator.ShardConfig(luceneQuery, searcher))
                );
                docPages = buildDocPages(windows);
            }
            case "esqlWhere" -> {
                buildIndex(indexDocs);
                // Pre-rewrite so the slice queue doesn't re-expand a fuzzy automaton every op.
                luceneQuery = searcher.rewrite(buildLuceneQuery());
                whereShardContexts = new IndexedByShardIdFromSingleton<>(new BenchmarkShardContext());
            }
            case "dslCount" -> {
                buildIndex(indexDocs);
                // Same COMPLETE_NO_SCORES Weight as the ES|QL evaluator, compiled once. Timed op is just the scan.
                countWeight = searcher.createWeight(searcher.rewrite(buildLuceneQuery()), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            }
            case "dslScored" -> {
                buildIndex(indexDocs);
                // COMPLETE so we actually compute BM25. Weight compiled once.
                scoreWeight = searcher.createWeight(searcher.rewrite(buildLuceneQuery()), ScoreMode.COMPLETE, 1.0f);
            }
            case "dslMemIndex" -> {
                luceneQuery = buildLuceneQuery();
                memoryIndex = new MemoryIndex();
            }
            default -> throw new IllegalArgumentException("unknown engine: " + engine);
        }

        validateSetup();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (runtimeEvaluator != null) {
            runtimeEvaluator.close();
        }
        if (runtimePage != null) {
            runtimePage.releaseBlocks();
        }
        if (docPages != null) {
            for (Page p : docPages) {
                p.releaseBlocks();
            }
        }
        IOUtils.close(reader, directory);
    }

    @Benchmark
    public void run(Blackhole bh) throws IOException {
        switch (engine) {
            case "esqlRuntime" -> {
                try (BooleanBlock result = (BooleanBlock) runtimeEvaluator.eval(runtimePage)) {
                    bh.consume(result);
                }
            }
            case "esqlEval" -> {
                // New evaluator per op; the cached BulkScorer only moves forward.
                ExpressionEvaluator evaluator = evalFactory.get(driverContext);
                try {
                    for (Page page : docPages) {
                        try (BooleanBlock result = (BooleanBlock) evaluator.eval(page)) {
                            bh.consume(result);
                        }
                    }
                } finally {
                    evaluator.close();
                }
            }
            case "esqlWhere" -> bh.consume(drainWhere());
            case "dslCount" -> bh.consume(countMatches(countWeight));
            case "dslScored" -> bh.consume(scoreTopK());
            case "dslMemIndex" -> bh.consume(countMemoryIndex());
            default -> throw new IllegalArgumentException("unknown engine: " + engine);
        }
    }

    /** One {@link LuceneSourceOperator} pass over the shared index. Returns matching docs emitted. */
    private long drainWhere() throws IOException {
        SourceOperator operator = buildWhereFactory().get(driverContext);
        long emitted = 0;
        try {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    emitted += page.getPositionCount();
                    page.releaseBlocks();
                }
            }
        } finally {
            operator.close();
        }
        return emitted;
    }

    private LuceneSourceOperator.Factory buildWhereFactory() {
        return new LuceneSourceOperator.Factory(
            whereShardContexts,
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(luceneQuery, List.of())),
            DataPartitioning.SHARD,
            DataPartitioning.AutoStrategy.DEFAULT,
            0,                       // unused for SHARD
            1,                       // taskConcurrency
            MAX_PAGE_SIZE,
            LuceneOperator.NO_LIMIT,
            false,                   // needsScore
            () -> 0L,
            1,                       // unused for SHARD
            QueryWarnings.NOOP
        );
    }

    private int indexWindows() {
        return Math.clamp(INDEX_TOKEN_BUDGET / (textTokens * BLOCK_LENGTH), MIN_INDEX_WINDOWS, MAX_INDEX_WINDOWS);
    }

    private Page buildRuntimePage() {
        var builder = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
        for (String content : contents) {
            builder.appendBytesRef(new BytesRef(content));
        }
        return new Page(builder.build().asBlock());
    }

    /** Consecutive {@link #BLOCK_LENGTH}-doc pages in doc-id order, so the bulk scorer can walk the index once. */
    private Page[] buildDocPages(int windows) {
        Page[] built = new Page[windows];
        for (int w = 0; w < windows; w++) {
            int firstDoc = w * BLOCK_LENGTH;
            try (DocVector.FixedBuilder builder = DocVector.newFixedBuilder(blockFactory, BLOCK_LENGTH)) {
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.append(0, 0, firstDoc + i);
                }
                built[w] = new Page(builder.build(DocVector.config().singleSegmentNonDecreasing(true)).asBlock());
            }
        }
        return built;
    }

    private ExpressionEvaluator buildRuntimeEvaluator() {
        Attribute field = new ReferenceAttribute(Source.EMPTY, FIELD, DataType.TEXT);
        Expression expr = matchExpression(field);
        Layout.Builder layoutBuilder = new Layout.Builder();
        layoutBuilder.append(List.of(field));
        ExpressionEvaluator.Factory factory = EvalMapper.toEvaluator(FOLD_CONTEXT, expr, layoutBuilder.build());
        Class<?> expected = expectedEvaluatorClass();
        if (factory.getClass().getEnclosingClass() != expected) {
            throw new AssertionError("query [" + query + "] built [" + factory + "], expected a " + expected.getSimpleName() + " factory");
        }
        return factory.get(driverContext);
    }

    private record Shape(boolean phrase, String queryString, Map<String, Object> options) {}

    private Shape shape() {
        return switch (query) {
            case "phrase", "phraseMiss" -> new Shape(true, PHRASE_TEXT, Map.of());
            case "phraseSlop" -> new Shape(true, PHRASE_TEXT, Map.of("slop", "1"));
            case "match" -> new Shape(false, PHRASE_TEXT, Map.of());
            case "matchAnd" -> new Shape(false, PHRASE_TEXT, Map.of("operator", "AND"));
            case "matchFuzzy" -> new Shape(false, FUZZY_TEXT, Map.of("fuzziness", "1"));
            default -> throw new IllegalArgumentException("unknown query: " + query);
        };
    }

    private Expression matchExpression(Attribute field) {
        Shape shape = shape();
        Literal queryValue = Literal.keyword(Source.EMPTY, shape.queryString());
        MapExpression options = mapOptions(shape.options());
        return shape.phrase()
            ? new MatchPhrase(Source.EMPTY, field, queryValue, options)
            : new Match(Source.EMPTY, field, queryValue, options);
    }

    private Class<?> expectedEvaluatorClass() {
        // No options → cheap token-stream matcher; any option goes through a Lucene query.
        return shape().options().isEmpty() ? RuntimeSearchTextEvaluator.class : RuntimeSearchTextWithLuceneQueryEvaluator.class;
    }

    /** null, not empty: Match/MatchPhrase treat {@code options() == null} as "no options". */
    private static MapExpression mapOptions(Map<String, Object> options) {
        if (options.isEmpty()) {
            return null;
        }
        List<Expression> keyValues = new ArrayList<>(options.size() * 2);
        options.forEach((key, value) -> {
            keyValues.add(Literal.keyword(Source.EMPTY, key));
            keyValues.add(Literal.keyword(Source.EMPTY, value.toString()));
        });
        return new MapExpression(Source.EMPTY, keyValues);
    }

    private Query buildLuceneQuery() throws IOException {
        return toLuceneQuery(shape());
    }

    private Query toLuceneQuery(Shape shape) throws IOException {
        QueryBuilder builder = shape.phrase()
            ? new MatchPhraseQuery(Source.EMPTY, FIELD, shape.queryString(), shape.options()).toQueryBuilder()
            : new MatchQuery(Source.EMPTY, FIELD, shape.queryString(), shape.options()).toQueryBuilder();
        return builder.toQuery(RuntimeSearchExecutionContext.create(List.of(FIELD), analyzer));
    }

    /**
     * Fail if options didn't change the Lucene query. Hit counts miss {@code matchAnd}: AND and OR both match
     * every row, so without this we'd be measuring OR and not know it.
     */
    private void validateQueryShape() throws IOException {
        Shape shape = shape();
        if (shape.options().isEmpty()) {
            return;
        }
        Query withOptions = toLuceneQuery(shape);
        Query withoutOptions = toLuceneQuery(new Shape(shape.phrase(), shape.queryString(), Map.of()));
        if (withOptions.equals(withoutOptions)) {
            throw new AssertionError(
                "query [" + query + "] options " + shape.options() + " left the Lucene query unchanged [" + withOptions + "]"
            );
        }
    }

    private void buildIndex(int docCount) throws IOException {
        directory = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        try (IndexWriter writer = new IndexWriter(directory, iwc)) {
            FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
            fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            fieldType.freeze();
            for (int i = 0; i < docCount; i++) {
                Document doc = new Document();
                doc.add(new Field(FIELD, contents[i % BLOCK_LENGTH], fieldType));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
        // Phrase/fuzzy weights are cacheable; a long run would start measuring the bitset cache instead.
        searcher.setQueryCache(null);
    }

    /** Count by walking postings. Don't use {@link IndexSearcher#count}: it can answer from doc freqs. */
    private long countMatches(Weight weight) throws IOException {
        long hits = 0;
        for (LeafReaderContext leaf : reader.leaves()) {
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            DocIdSetIterator it = scorer.iterator();
            for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                hits++;
            }
        }
        return hits;
    }

    /** BM25 top-{@value #TOP_K} over the index. Returns the smallest kept score so scoring isn't dead-code-eliminated. */
    private double scoreTopK() throws IOException {
        PriorityQueue<Double> topK = new PriorityQueue<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            Scorer scorer = scoreWeight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            DocIdSetIterator it = scorer.iterator();
            for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                double score = scorer.score();
                if (topK.size() < TOP_K) {
                    topK.add(score);
                } else if (score > topK.peek()) {
                    topK.poll();
                    topK.add(score);
                }
            }
        }
        return topK.isEmpty() ? 0.0 : topK.peek();
    }

    private int countMemoryIndex() {
        int hits = 0;
        for (String content : contents) {
            memoryIndex.reset();
            memoryIndex.addField(FIELD, content, analyzer);
            // Filter only: search() > 0, not a scored top-1.
            if (memoryIndex.search(luceneQuery) > 0.0f) {
                hits++;
            }
        }
        return hits;
    }

    private void validateSetup() throws IOException {
        boolean miss = "phraseMiss".equals(query);
        long expected = switch (engine) {
            case "esqlRuntime", "dslMemIndex" -> miss ? 0 : BLOCK_LENGTH;
            case "esqlEval", "esqlWhere", "dslCount", "dslScored" -> miss ? 0 : indexDocs;
            default -> throw new IllegalArgumentException("unknown engine: " + engine);
        };
        // Two passes. A leftover exhausted scorer would hit on the first and 0 on the second.
        for (int pass = 1; pass <= 2; pass++) {
            long hits = countHits();
            if (hits != expected) {
                throw new AssertionError(
                    "self-test failed for engine ["
                        + engine
                        + "] query ["
                        + query
                        + "] textTokens ["
                        + textTokens
                        + "] on pass ["
                        + pass
                        + "]: expected ["
                        + expected
                        + "] hits but found ["
                        + hits
                        + "]"
                );
            }
        }
    }

    private long countHits() throws IOException {
        return switch (engine) {
            case "esqlRuntime" -> {
                try (BooleanBlock result = (BooleanBlock) runtimeEvaluator.eval(runtimePage)) {
                    yield countTrue(result);
                }
            }
            case "esqlEval" -> {
                ExpressionEvaluator evaluator = evalFactory.get(driverContext);
                try {
                    long hits = 0;
                    for (Page page : docPages) {
                        try (BooleanBlock result = (BooleanBlock) evaluator.eval(page)) {
                            hits += countTrue(result);
                        }
                    }
                    yield hits;
                } finally {
                    evaluator.close();
                }
            }
            case "esqlWhere" -> drainWhere();
            case "dslCount" -> countMatches(countWeight);
            case "dslScored" -> countMatches(scoreWeight);
            case "dslMemIndex" -> countMemoryIndex();
            default -> throw new IllegalArgumentException("unknown engine: " + engine);
        };
    }

    private static int countTrue(BooleanBlock result) {
        int hits = 0;
        for (int i = 0; i < result.getPositionCount(); i++) {
            if (result.isNull(i) == false && result.getBoolean(result.getFirstValueIndex(i))) {
                hits++;
            }
        }
        return hits;
    }

    private String row(Random random) {
        String[] tokens = new String[textTokens];
        for (int i = 0; i < textTokens; i++) {
            tokens[i] = randomToken(random);
        }
        int phraseStart;
        int phraseEnd;
        switch (query) {
            case "phraseMiss" -> {
                // Terms present but never adjacent, so both engines do real work on a miss.
                phraseStart = random.nextInt(textTokens - 4);
                tokens[phraseStart] = PHRASE[0];
                tokens[phraseStart + 2] = PHRASE[1];
                tokens[phraseStart + 4] = PHRASE[2];
                phraseEnd = phraseStart + 4;
            }
            case "phraseSlop" -> {
                phraseStart = random.nextInt(textTokens - 3);
                tokens[phraseStart] = PHRASE[0];
                tokens[phraseStart + 2] = PHRASE[1];
                tokens[phraseStart + 3] = PHRASE[2];
                phraseEnd = phraseStart + 3;
            }
            case "phrase", "match", "matchAnd", "matchFuzzy" -> {
                phraseStart = random.nextInt(textTokens - PHRASE.length + 1);
                for (int j = 0; j < PHRASE.length; j++) {
                    tokens[phraseStart + j] = PHRASE[j];
                }
                phraseEnd = phraseStart + PHRASE.length - 1;
            }
            default -> throw new IllegalArgumentException("unknown query: " + query);
        }

        StringBuilder text = new StringBuilder();
        boolean sentenceStart = true;
        int remaining = sentenceLength(random);
        for (int i = 0; i < textTokens; i++) {
            if (i > 0) {
                text.append(' ');
            }
            String token = tokens[i];
            if (sentenceStart) {
                text.append(Character.toUpperCase(token.charAt(0))).append(token, 1, token.length());
                sentenceStart = false;
            } else {
                text.append(token);
            }
            boolean canBreak = i < phraseStart || i >= phraseEnd;
            if (--remaining <= 0 && canBreak) {
                text.append('.');
                remaining = sentenceLength(random);
                sentenceStart = true;
            }
        }
        return text.toString();
    }

    private static int sentenceLength(Random random) {
        return 8 + random.nextInt(9);
    }

    private static String randomToken(Random random) {
        String token;
        do {
            int length = 4 + random.nextInt(2);
            StringBuilder sb = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                sb.append((char) ('a' + random.nextInt(26)));
            }
            token = sb.toString();
        } while (PHRASE_TERMS.contains(token));
        return token;
    }

    /** Bare {@link ShardContext} for a filter-only {@link LuceneSourceOperator} scan. */
    private final class BenchmarkShardContext implements ShardContext {
        private final ShardSearchStats stats = new ShardSearchStats(
            new SearchStatsSettings(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );

        @Override
        public int index() {
            return 0;
        }

        @Override
        public IndexSearcher searcher() {
            return searcher;
        }

        @Override
        public String shardIdentifier() {
            return "bench";
        }

        @Override
        public ShardSearchStats stats() {
            return stats;
        }

        @Override
        public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
            throw new UnsupportedOperationException("no sort in a filter-context scan");
        }

        @Override
        public SourceLoader newSourceLoader(Set<String> sourcePaths) {
            throw new UnsupportedOperationException("no _source loading in a filter-context scan");
        }

        @Override
        public BlockLoader blockLoader(
            String name,
            boolean asUnsupportedSource,
            MappedFieldType.FieldExtractPreference fieldExtractPreference,
            BlockLoaderFunctionConfig blockLoaderFunctionConfig,
            Warnings warnings,
            ByteSizeValue blockLoaderSizeOrdinals,
            ByteSizeValue blockLoaderSizeScript
        ) {
            throw new UnsupportedOperationException("no field values loaded in a filter-context scan");
        }

        @Override
        public MappedFieldType fieldType(String name) {
            throw new UnsupportedOperationException("no field types resolved in a filter-context scan");
        }

        @Override
        public void incRef() {}

        @Override
        public boolean tryIncRef() {
            return true;
        }

        @Override
        public boolean decRef() {
            return false;
        }

        @Override
        public boolean hasReferences() {
            return true;
        }
    }
}
