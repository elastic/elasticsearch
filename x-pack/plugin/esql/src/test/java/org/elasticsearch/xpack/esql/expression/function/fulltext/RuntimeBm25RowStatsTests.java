/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSlice;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests {@link RuntimeBm25RowStats}, the per-row statistics that the BM25 statistics pass adds up into N, the summed
 * length and the document frequency of each query term. Each test runs the real evaluator over real blocks.
 */
public class RuntimeBm25RowStatsTests extends AbstractRuntimeSearchEvaluatorTests {

    private static final Expression FIELD = EsqlTestUtils.referenceAttribute("content", DataType.TEXT);

    public void testDataTypeIsLong() {
        assertThat(rowStats("standard", List.of("fox")).dataType(), equalTo(DataType.LONG));
    }

    public void testSingleValuedRows() {
        Long[][] stats = evaluate("standard", List.of("fox", "missing", "quick"), Map.of(), rows("The quick fox", "fox fox"));

        // [hasToken, tokenCount, present(fox), present(missing), present(quick)]
        assertThat(stats[0], arrayContaining(1L, 3L, 1L, 0L, 1L));
        assertThat(stats[1], arrayContaining(1L, 2L, 1L, 0L, 0L));
    }

    public void testMultiValuedRowIsOneDocument() {
        Long[][] stats = evaluate("standard", List.of("fox", "quick"), Map.of(), factory -> {
            try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(1)) {
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("quick"));
                builder.appendBytesRef(new BytesRef("brown fox"));
                builder.endPositionEntry();
                return builder.build();
            }
        });

        assertThat(stats[0], arrayContaining(1L, 3L, 1L, 1L));
    }

    public void testNullAndZeroTokenRows() {
        Long[][] stats = evaluate("stop", List.of("fox"), Map.of(), factory -> {
            try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(4)) {
                builder.appendNull();
                builder.appendBytesRef(new BytesRef("the and a"));
                builder.appendBytesRef(new BytesRef(""));
                builder.appendBytesRef(new BytesRef("fox"));
                return builder.build();
            }
        });

        assertThat(stats[0], nullValue());
        assertThat(stats[1], arrayContaining(0L, 0L, 0L));
        assertThat(stats[2], arrayContaining(0L, 0L, 0L));
        assertThat(stats[3], arrayContaining(1L, 1L, 1L));
    }

    /**
     * Stacked tokens (here a synonym at the same position) count toward the token count, as they count toward an
     * index's {@code sumTotalTermFreq}. The scoring pass's norms discount them separately.
     */
    public void testStackedTokensCountTowardTokenCount() throws IOException {
        Map<String, Analyzer> analyzers = Map.of("test_synonyms", synonymAnalyzer());
        Long[][] stats = evaluate("test_synonyms", List.of("auto", "car", "red"), analyzers, rows("red car"));

        assertThat(stats[0], arrayContaining(1L, 3L, 1L, 1L, 1L));
    }

    /**
     * The statistics pass analyzes each value once, whatever the number of query terms. The prototype analyzed each
     * row once per term.
     */
    public void testOneAnalysisPerValue() {
        CountingAnalyzer counting = new CountingAnalyzer(new StandardAnalyzer());
        List<String> terms = List.of("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8");
        evaluate("counting", terms, Map.of("counting", counting), factory -> {
            try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(3)) {
                builder.appendBytesRef(new BytesRef("a1 a2"));
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("a3"));
                builder.appendBytesRef(new BytesRef("a4 a5"));
                builder.endPositionEntry();
                builder.appendNull();
                return builder.build();
            }
        });

        assertThat(counting.analyses.get(), equalTo(3));
    }

    /**
     * Open Question 2 of the design: the statistics pass reads position {@code i} of each row with
     * {@code MV_SLICE(row, i, i)}. Each slice must return the value the row put at that position, on every page.
     */
    public void testMvSliceReadsEachPositionOnEveryPage() {
        List<String> terms = List.of("brown", "fox", "quick");
        RuntimeBm25RowStats rowStats = rowStats("standard", terms);
        int pages = between(2, 4);
        for (int page = 0; page < pages; page++) {
            String[] values = new String[between(1, 20)];
            for (int r = 0; r < values.length; r++) {
                values[r] = String.join(" ", randomSubsetOf(between(1, 4), List.of("quick", "brown", "fox", "jumps", "over")));
            }
            Long[][] expected = evaluate(rowStats, toEvaluator(), rows(values));
            for (int i = 0; i < terms.size() + 2; i++) {
                Long[] slice = evaluateSlice(rowStats, i, rows(values));
                for (int r = 0; r < values.length; r++) {
                    assertThat("page " + page + ", row " + r + ", position " + i, slice[r], equalTo(expected[r][i]));
                }
            }
        }
    }

    private static RuntimeBm25RowStats rowStats(String analyzer, List<String> terms) {
        return new RuntimeBm25RowStats(
            Source.EMPTY,
            FIELD,
            Literal.keyword(Source.EMPTY, analyzer),
            RuntimeBm25RowStats.termsLiteral(Source.EMPTY, terms.stream().map(BytesRef::new).toList())
        );
    }

    private Long[][] evaluate(
        String analyzer,
        List<String> terms,
        Map<String, Analyzer> customAnalyzers,
        Function<BlockFactory, Block> fieldBuilder
    ) {
        return evaluate(rowStats(analyzer, terms), toEvaluatorWith(customAnalyzers), fieldBuilder);
    }

    private Long[][] evaluate(
        RuntimeBm25RowStats rowStats,
        EvaluatorMapper.ToEvaluator toEvaluator,
        Function<BlockFactory, Block> fieldBuilder
    ) {
        return evaluateLongs(rowStats.toEvaluator(toEvaluator), fieldBuilder);
    }

    private Long[] evaluateSlice(RuntimeBm25RowStats rowStats, int index, Function<BlockFactory, Block> fieldBuilder) {
        EvaluatorMapper.ToEvaluator base = toEvaluator();
        EvaluatorMapper.ToEvaluator sliceInputs = new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                if (expression instanceof Literal literal) {
                    return constantInt((Integer) literal.value());
                }
                return expression == rowStats ? rowStats.toEvaluator(base) : base.apply(expression);
            }

            @Override
            public FoldContext foldCtx() {
                return base.foldCtx();
            }

            @Override
            public Analyzer getAnalyzer(String name) {
                return base.getAnalyzer(name);
            }
        };
        MvSlice slice = new MvSlice(
            Source.EMPTY,
            rowStats,
            new Literal(Source.EMPTY, index, DataType.INTEGER),
            new Literal(Source.EMPTY, index, DataType.INTEGER)
        );
        Long[][] values = evaluateLongs(slice.toEvaluator(sliceInputs), fieldBuilder);
        Long[] firstValues = new Long[values.length];
        for (int r = 0; r < values.length; r++) {
            assertThat(values[r].length, equalTo(1));
            firstValues[r] = values[r][0];
        }
        return firstValues;
    }

    private Long[][] evaluateLongs(ExpressionEvaluator.Factory factory, Function<BlockFactory, Block> fieldBuilder) {
        DriverContext context = driverContext();
        Block fieldBlock = fieldBuilder.apply(context.blockFactory());
        Page page = new Page(fieldBlock);
        try (ExpressionEvaluator evaluator = factory.get(context); LongBlock result = (LongBlock) evaluator.eval(page)) {
            Long[][] out = new Long[result.getPositionCount()][];
            for (int p = 0; p < out.length; p++) {
                if (result.isNull(p)) {
                    continue;
                }
                int first = result.getFirstValueIndex(p);
                out[p] = new Long[result.getValueCount(p)];
                for (int v = 0; v < out[p].length; v++) {
                    out[p][v] = result.getLong(first + v);
                }
            }
            return out;
        } finally {
            page.releaseBlocks();
        }
    }

    private static Function<BlockFactory, Block> rows(String... values) {
        return factory -> {
            try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(values.length)) {
                for (String value : values) {
                    builder.appendBytesRef(new BytesRef(value));
                }
                return builder.build();
            }
        };
    }

    private static ExpressionEvaluator.Factory constantInt(int value) {
        return context -> new ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                return context.blockFactory().newConstantIntBlockWith(value, page.getPositionCount());
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}
        };
    }

    /**
     * The base harness resolves analyzer names through the test registry. This adds analyzers the registry does not
     * have, such as a synonym analyzer or a counting wrapper.
     */
    static EvaluatorMapper.ToEvaluator toEvaluatorWith(Map<String, Analyzer> customAnalyzers) {
        EvaluatorMapper.ToEvaluator base = toEvaluator();
        return new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                return base.apply(expression);
            }

            @Override
            public FoldContext foldCtx() {
                return base.foldCtx();
            }

            @Override
            public Analyzer getAnalyzer(String name) {
                Analyzer custom = customAnalyzers.get(name);
                return custom != null ? custom : base.getAnalyzer(name);
            }
        };
    }

    /**
     * Lowercased standard tokens, with {@code car} and {@code auto} as single-token synonyms of each other. A synonym
     * is emitted at the same position as the original token.
     */
    static Analyzer synonymAnalyzer() throws IOException {
        SynonymMap.Builder builder = new SynonymMap.Builder(true);
        builder.add(new CharsRef("car"), new CharsRef("auto"), true);
        builder.add(new CharsRef("auto"), new CharsRef("car"), true);
        SynonymMap synonyms = builder.build();
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new StandardTokenizer();
                TokenStream stream = new SynonymGraphFilter(new LowerCaseFilter(tokenizer), synonyms, true);
                return new TokenStreamComponents(tokenizer, stream);
            }
        };
    }

    /**
     * Counts how many times a value is analyzed. Each call to {@link Analyzer#tokenStream} wraps the input reader
     * once, so counting the wrapped readers counts analyses.
     */
    private static final class CountingAnalyzer extends AnalyzerWrapper {
        private final Analyzer delegate;
        private final AtomicInteger analyses = new AtomicInteger();

        CountingAnalyzer(Analyzer delegate) {
            super(delegate.getReuseStrategy());
            this.delegate = delegate;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        protected Reader wrapReader(String fieldName, Reader reader) {
            analyses.incrementAndGet();
            return reader;
        }
    }
}
