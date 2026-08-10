/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.LoadFromPageEvaluator;
import org.elasticsearch.compute.operator.HighlightConfig;
import org.elasticsearch.compute.operator.HighlightOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.lucene.search.uhighlight.CustomPassageFormatter;
import org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.elasticsearch.lucene.search.uhighlight.QueryMaxAnalyzedOffset;
import org.elasticsearch.lucene.search.uhighlight.Snippet;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.logical.HighlightOptions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/** Compares runtime ES|QL {@code HIGHLIGHT} with the Query DSL unified highlighter. */
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 7, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class HighlightVsDslBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final int BLOCK_LENGTH = 128;
    private static final String FIELD = "content";

    /** Single term used by the term/prefix/wildcard/regexp shapes. */
    private static final String TERM = "fox";

    /** Planted verbatim in every row so the match and match_phrase shapes have a hit; ends with {@link #TERM}. */
    private static final String[] PHRASE = { "quick", "brown", TERM };

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final FoldContext FOLD_CONTEXT = FoldContext.small();

    private static final int MAX_ANALYZED_OFFSET = IndexSettings.MAX_ANALYZED_OFFSET_SETTING.get(Settings.EMPTY);

    /** Number of tokens in each row. */
    @Param({ "128", "1024", "8192" })
    public int textTokens;

    /** Highlighter under test. */
    @Param({ "esql", "dsl" })
    public String engine;

    /** Query shape. {@code term}, {@code match} and {@code match_phrase} are the normal shapes; the rest are multi-term. */
    @Param({ "term", "match", "match_phrase", "prefix", "wildcard", "regexp" })
    public String query;

    private String[] contents;
    private int maxPassages;

    private Page page;
    private HighlightOperator highlightOperator;

    private Directory directory;
    private DirectoryReader reader;
    private LeafReader leafReader;
    private CustomUnifiedHighlighter dslHighlighter;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        HighlightOptions options = HighlightOptions.from(null, FOLD_CONTEXT);
        maxPassages = options.numberOfFragments();

        Random random = new Random(42L);
        contents = new String[BLOCK_LENGTH];
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            contents[i] = row(random);
        }

        Query luceneQuery = buildQuery(query);

        if ("esql".equals(engine)) {
            page = buildPage();
            highlightOperator = newHighlightOperator(options, luceneQuery);
        } else if ("dsl".equals(engine)) {
            buildIndex();
            dslHighlighter = newDslHighlighter(luceneQuery);
        } else {
            throw new IllegalArgumentException("unknown engine: " + engine);
        }

        validateSetup();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (page != null) {
            page.releaseBlocks();
        }
        if (highlightOperator != null) {
            highlightOperator.close();
        }
        IOUtils.close(reader, directory);
    }

    @Benchmark
    @OperationsPerInvocation(BLOCK_LENGTH)
    public void run(Blackhole bh) throws IOException {
        if (highlightOperator != null) {
            highlightOperator.addInput(page);
            bh.consume(highlightOperator.getOutput());
        } else {
            for (int i = 0; i < BLOCK_LENGTH; i++) {
                final String content = contents[i];
                bh.consume(dslHighlighter.highlightField(leafReader, i, () -> content));
            }
        }
    }

    private Page buildPage() {
        var builder = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
        for (String content : contents) {
            builder.appendBytesRef(new BytesRef(content));
        }
        return new Page(builder.build().asBlock());
    }

    private HighlightOperator newHighlightOperator(HighlightOptions options, Query luceneQuery) {
        Analyzer analyzer = new StandardAnalyzer();
        HighlightConfig config = new HighlightConfig(
            query,
            options.preTag(),
            options.postTag(),
            options.encoder(),
            options.numberOfFragments(),
            options.fragmentSize(),
            options.noMatchSize(),
            HighlightOptions.BOUNDARY_SCANNER_WORD.equals(options.boundaryScanner()),
            options.boundaryScannerLocale(),
            HighlightOptions.ORDER_SCORE.equals(options.order()),
            options.analyzerName(),
            options.maxAnalyzedOffset()
        ).withExecutionContext(analyzer, luceneQuery, List.of(FIELD));
        return new HighlightOperator(blockFactory, config, new ExpressionEvaluator[] { new LoadFromPageEvaluator(0) });
    }

    private void buildIndex() throws IOException {
        directory = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        try (IndexWriter writer = new IndexWriter(directory, iwc)) {
            FieldType fieldType = new FieldType(TextField.TYPE_STORED);
            fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
            fieldType.freeze();
            for (String content : contents) {
                Document doc = new Document();
                doc.add(new Field(FIELD, content, fieldType));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        leafReader = reader.leaves().get(0).reader();
    }

    private CustomUnifiedHighlighter newDslHighlighter(Query luceneQuery) {
        Analyzer analyzer = new StandardAnalyzer();
        IndexSearcher searcher = new IndexSearcher(reader);
        UnifiedHighlighter.Builder builder = UnifiedHighlighter.builder(searcher, analyzer);
        builder.withFormatter(new CustomPassageFormatter("<em>", "</em>", new DefaultEncoder(), maxPassages));
        builder.withBreakIterator(() -> BreakIterator.getSentenceInstance(Locale.ROOT));
        builder.withFieldMatcher(FIELD::equals);
        return new CustomUnifiedHighlighter(
            builder,
            UnifiedHighlighter.OffsetSource.ANALYSIS,
            Locale.ROOT,
            "index",
            FIELD,
            luceneQuery,
            0,
            maxPassages,
            MAX_ANALYZED_OFFSET,
            QueryMaxAnalyzedOffset.create(MAX_ANALYZED_OFFSET, MAX_ANALYZED_OFFSET),
            true,
            true
        );
    }

    private static Query buildQuery(String query) {
        return switch (query) {
            case "term" -> new TermQuery(new Term(FIELD, TERM));
            case "match" -> {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                for (String word : PHRASE) {
                    builder.add(new TermQuery(new Term(FIELD, word)), BooleanClause.Occur.SHOULD);
                }
                yield builder.build();
            }
            case "match_phrase" -> {
                PhraseQuery.Builder builder = new PhraseQuery.Builder();
                for (int i = 0; i < PHRASE.length; i++) {
                    builder.add(new Term(FIELD, PHRASE[i]), i);
                }
                yield builder.build();
            }
            case "prefix" -> new PrefixQuery(new Term(FIELD, "fo"));
            case "wildcard" -> new WildcardQuery(new Term(FIELD, "*ox*"));
            case "regexp" -> new RegexpQuery(new Term(FIELD, "f.x"));
            default -> throw new IllegalArgumentException("unknown query: " + query);
        };
    }

    private String row(Random random) {
        String[] tokens = new String[textTokens];
        for (int i = 0; i < textTokens; i++) {
            tokens[i] = randomToken(random);
        }
        int phraseStart = random.nextInt(textTokens - PHRASE.length + 1);
        for (int j = 0; j < PHRASE.length; j++) {
            tokens[phraseStart + j] = PHRASE[j];
        }
        int phraseEnd = phraseStart + PHRASE.length - 1;

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
            // Defer the sentence break until after the planted phrase so it stays within one passage.
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
        int length = 4 + random.nextInt(2);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }

    private void validateSetup() throws IOException {
        int highlighted = 0;
        boolean sawTag = false;
        if (highlightOperator != null) {
            highlightOperator.addInput(page);
            Page output = highlightOperator.getOutput();
            BytesRefBlock block = output.getBlock(output.getBlockCount() - 1);
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i) == false) {
                    highlighted++;
                    sawTag |= block.getBytesRef(block.getFirstValueIndex(i), scratch).utf8ToString().contains("<em>");
                }
            }
            block.close();
        } else {
            for (int i = 0; i < BLOCK_LENGTH; i++) {
                final String content = contents[i];
                Snippet[] snippets = dslHighlighter.highlightField(leafReader, i, () -> content);
                if (snippets != null && snippets.length > 0) {
                    highlighted++;
                    for (Snippet snippet : snippets) {
                        sawTag |= snippet.getText().contains("<em>");
                    }
                }
            }
        }
        if (highlighted != BLOCK_LENGTH || sawTag == false) {
            throw new AssertionError(
                "self-test failed for engine ["
                    + engine
                    + "] query ["
                    + query
                    + "] textTokens ["
                    + textTokens
                    + "]: expected ["
                    + BLOCK_LENGTH
                    + "] highlighted rows with an <em> tag but found ["
                    + highlighted
                    + "] highlighted, sawTag="
                    + sawTag
            );
        }
    }
}
