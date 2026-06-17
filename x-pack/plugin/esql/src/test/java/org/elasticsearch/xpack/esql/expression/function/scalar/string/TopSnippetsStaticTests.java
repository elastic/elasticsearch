/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.common.chunks.MemoryIndexChunkScorer;
import org.elasticsearch.xpack.core.common.chunks.ScoredChunk;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.buildLayout;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.driverContext;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.evaluator;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.field;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.row;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.TopSnippets.DEFAULT_NUM_SNIPPETS;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.TopSnippets.DEFAULT_WORD_SIZE;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.TopSnippetsTests.createOptions;
import static org.elasticsearch.xpack.esql.expression.function.scalar.util.ChunkUtils.chunkText;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Non-parameterized tests for TopSnippets.
 */
public class TopSnippetsStaticTests extends ESTestCase {
    private static final String PARAGRAPH_INPUT = """
        The Adirondacks, a vast mountain region in northern New York, offer a breathtaking mix of rugged wilderness, serene lakes,
        and charming small towns. Spanning over six million acres, the Adirondack Park is larger than Yellowstone, Yosemite, and the
        Grand Canyon combined, yet it's dotted with communities where people live, work, and play amidst nature. Visitors come year-round
        to hike High Peaks trails, paddle across mirror-like waters, or ski through snow-covered forests. The area's pristine beauty,
        rich history, and commitment to conservation create a unique balance between wild preservation and human presence, making
        the Adirondacks a timeless escape into the tranquility of nature.
        """;
    private static AnalysisRegistry analysisRegistry;

    @BeforeClass
    public static void setupAnalysisRegistry() throws IOException {
        analysisRegistry = new AnalysisModule(
            TestEnvironment.newEnvironment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build()
            ),
            List.of(new org.elasticsearch.analysis.common.CommonAnalysisPlugin()),
            new StablePluginsRegistry()
        ).getAnalysisRegistry();
    }

    @AfterClass
    public static void tearDownAnalysisRegistry() {
        analysisRegistry = null;
    }

    public void testDefaultOptions() {
        String query = "wilderness";
        verifySnippets(query, null, null, 1);
    }

    public void testSpecifiedOptions() {
        // We can't randomize here, because we're testing on specifically specified options that are variable.
        String query = "nature";
        int numWords = 25;
        int numSnippets = 3;
        int expectedNumChunks = 2;
        verifySnippets(query, numSnippets, numWords, expectedNumChunks);
    }

    public void testRandomOptions() {
        String query = "park"; // Ensure we get a match
        int numSnippets = randomIntBetween(1, 2);
        int numWords = randomIntBetween(20, 25);

        List<String> result = process(PARAGRAPH_INPUT, query, numSnippets, numWords);
        assertNotNull(result);
        assertFalse(result.isEmpty());
        // Actual results depend on options passed in
    }

    public void testNoMatches() {
        // Pick a random word from the paragraph to ensure we get matches
        String query = randomAlphaOfLengthBetween(10, 15);
        int numSnippets = randomIntBetween(1, 10);
        int numWords = randomIntBetween(20, 500);

        List<String> result = process(PARAGRAPH_INPUT, query, numSnippets, numWords);
        assertNull(result);
    }

    public void testOrderByScore() {
        List<String> chunks = List.of("We use elasticsearch for search.", "Elasticsearch elasticsearch elasticsearch powers our stack.");
        List<String> byScore = processMultivalueChunks(chunks, "elasticsearch", 2, "score");
        assertThat(byScore, hasSize(2));
        assertThat(byScore.get(0), equalTo(chunks.get(1)));
        assertThat(byScore.get(1), equalTo(chunks.get(0)));
    }

    public void testOrderNone() {
        List<String> chunks = List.of("We use elasticsearch for search.", "Elasticsearch elasticsearch elasticsearch powers our stack.");
        List<String> byDocument = processMultivalueChunks(chunks, "elasticsearch", 2, "none");
        assertThat(byDocument, hasSize(2));
        assertThat(byDocument.get(0), equalTo(chunks.get(0)));
        assertThat(byDocument.get(1), equalTo(chunks.get(1)));
    }

    public void testOrderDefaultIsScore() {
        List<String> chunks = List.of("We use elasticsearch for search.", "Elasticsearch elasticsearch elasticsearch powers our stack.");
        assertThat(
            processMultivalueChunks(chunks, "elasticsearch", 2, null),
            equalTo(processMultivalueChunks(chunks, "elasticsearch", 2, "score"))
        );
    }

    public void testSnippetsReturnedInScoringOrder() {
        String highRelevance = "Elasticsearch is a powerful search engine. "
            + "Elasticsearch supports full-text search and vector search. "
            + "Many companies rely on Elasticsearch for their search infrastructure.";

        String lowRelevance = "There are many search engines available today. "
            + "Elasticsearch is one option among several alternatives. "
            + "Choosing the right tool depends on your requirements.";

        String noRelevance = "The weather today is sunny and warm. "
            + "Perfect conditions for a walk in the park. "
            + "The temperature is expected to reach 25 degrees.";

        String query = "elasticsearch";

        String combinedText = noRelevance + " " + highRelevance + " " + lowRelevance;

        List<String> result = process(combinedText, query, 3, 50);

        assertThat(result, hasSize(2));
        assertThat(result.get(0), containsString("Elasticsearch is a powerful search engine"));
        assertThat(result.get(1), containsString("Elasticsearch is one option among several alternatives"));
    }

    private void verifySnippets(String query, Integer numSnippets, Integer numWords, int expectedNumChunksReturned) {
        int effectiveNumWords = numWords != null ? numWords : DEFAULT_WORD_SIZE;
        int effectiveNumSnippets = numSnippets != null ? numSnippets : DEFAULT_NUM_SNIPPETS;
        ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(effectiveNumWords, 0);

        MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();
        List<String> expected = scorer.scoreChunks(
            chunkText(PARAGRAPH_INPUT, chunkingSettings).stream().map(String::trim).toList(),
            query,
            effectiveNumSnippets,
            false
        ).stream().map(ScoredChunk::content).limit(effectiveNumSnippets).toList();

        List<String> result = process(PARAGRAPH_INPUT, query, effectiveNumSnippets, effectiveNumWords);
        assertThat(result.size(), equalTo(expectedNumChunksReturned));
        assertThat(result, equalTo(expected));
    }

    public void testHighlightDefaultTags() {
        String text = "The Adirondack Park is a beautiful wilderness area.";
        List<String> result = processWithHighlight(text, "park", 5, 300, true, null, null, null, null, null);
        assertThat(result, equalTo(List.of("The Adirondack <em>Park</em> is a beautiful wilderness area.")));
    }

    public void testHighlightCustomTags() {
        String text = "The Adirondack Park is a beautiful wilderness area.";
        List<String> result = processWithHighlight(text, "park", 5, 300, true, "<b>", "</b>", null, null, null);
        assertThat(result, equalTo(List.of("The Adirondack <b>Park</b> is a beautiful wilderness area.")));
    }

    public void testHighlightMultipleTerms() {
        String text = "Elasticsearch is a search engine. Lucene powers Elasticsearch.";
        List<String> result = processWithHighlight(text, "elasticsearch lucene", 5, 300, true, null, null, null, null, null);
        assertThat(result, equalTo(List.of("<em>Elasticsearch</em> is a search engine. <em>Lucene</em> powers <em>Elasticsearch</em>.")));
    }

    public void testHighlightFalseReturnsPlainText() {
        String text = "The Adirondack Park is a beautiful wilderness area.";
        List<String> withHighlight = processWithHighlight(text, "park", 5, 300, true, null, null, null, null, null);
        List<String> withoutHighlight = processWithHighlight(text, "park", 5, 300, false, null, null, null, null, null);
        List<String> noHighlightOption = process(text, "park", 5, 300);

        assertThat(withHighlight, equalTo(List.of("The Adirondack <em>Park</em> is a beautiful wilderness area.")));
        assertThat(withoutHighlight, equalTo(List.of("The Adirondack Park is a beautiful wilderness area.")));
        assertThat(noHighlightOption, equalTo(List.of("The Adirondack Park is a beautiful wilderness area.")));
    }

    public void testHighlightHtmlEncoder() {
        String text = "Use <b>bold</b> & special chars with the Ring.";
        List<String> result = processWithHighlight(text, "ring", 5, 300, true, null, null, "html", null, null);
        assertThat(result, equalTo(List.of("Use &lt;b&gt;bold&lt;&#x2F;b&gt; &amp; special chars with the <em>Ring</em>.")));
    }

    public void testWhitespaceAnalyzerDoesNotLowercaseTokens() {
        String text = "Walk your DOGs daily.";
        List<String> standardResult = processWithHighlight(text, "dogs", 5, 300, null, null, null, null, null, "standard");
        List<String> whitespaceResult = processWithHighlight(text, "dogs", 5, 300, null, null, null, null, null, "whitespace");
        assertNotNull(standardResult);
        assertNull(whitespaceResult);
    }

    public void testEnglishAnalyzerStemsRunningToRun() {
        String text = "The running club meets daily for long runs.";
        List<String> english = processWithHighlight(text, "run", 5, 0, true, null, null, null, null, "english");
        List<String> standard = processWithHighlight(text, "run", 5, 0, true, null, null, null, null, "standard");
        assertNotNull(english);
        assertNull(standard);
        assertThat(english.get(0), containsString("<em>running</em>"));
        assertThat(english.get(0), containsString("<em>runs</em>"));
    }

    public void testUnknownAnalyzerThrows() {
        String text = "Some text to analyze.";
        InvalidArgumentException e = expectThrows(
            InvalidArgumentException.class,
            () -> processWithHighlight(text, "text", 5, 300, null, null, null, null, null, "nonexistent_analyzer")
        );
        assertThat(e.getMessage(), containsString("'analyzer' must be a registered analyzer"));
        assertThat(e.getMessage(), containsString("nonexistent_analyzer"));
    }

    /**
     * {@code TOP_SNIPPETS} runs on the coordinator, so an omitted {@code analyzer} resolves to
     * {@link org.apache.lucene.analysis.standard.StandardAnalyzer}: query "return" matches only the
     * exact token "return", not "returns".
     */
    public void testHighlightNoStemmingUsesExactTermOnly() {
        String text = "The API returns results. Use the return value to continue.";
        List<String> result = processWithHighlight(text, "return", 5, 300, true, null, null, null, null, null);
        assertThat(result, equalTo(List.of("The API returns results. Use the <em>return</em> value to continue.")));
    }

    public void testFoldableWithoutOptions() {
        TopSnippets expr = new TopSnippets(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "The Adirondack Park is a beautiful wilderness area."),
            Literal.keyword(Source.EMPTY, "park"),
            null
        );
        assertTrue(expr.foldable());
        assertNotNull(expr.fold(FoldContext.small()));
    }

    public void testFoldableWithOptionsButNoAnalyzer() {
        MapExpression options = createOptions(3, 50, true, null, null, null, null, null);
        TopSnippets expr = new TopSnippets(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "The Adirondack Park is a beautiful wilderness area."),
            Literal.keyword(Source.EMPTY, "park"),
            options
        );
        assertTrue(expr.foldable());
        assertNotNull(expr.fold(FoldContext.small()));
    }

    public void testNotFoldableWithAnalyzer() {
        // The synthetic ToEvaluator built inside EvaluatorMapper#fold has no AnalysisRegistry,
        // so an explicit analyzer name disables folding.
        MapExpression options = createOptions(null, null, null, null, null, null, null, "english");
        TopSnippets expr = new TopSnippets(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "The Adirondack Park is a beautiful wilderness area."),
            Literal.keyword(Source.EMPTY, "park"),
            options
        );
        assertFalse(expr.foldable());
    }

    public void testHighlightPreservesWholeChunk() {
        // A long chunk with many matching terms — highlighting must return the entire chunk as-is
        // (with markup), not split it into smaller passages.
        String text = "Elasticsearch is a search engine built on Lucene. "
            + "Elasticsearch supports distributed search across many nodes. "
            + "Elasticsearch provides near real-time search capabilities. "
            + "Elasticsearch scales horizontally for large search workloads.";
        List<String> result = processWithHighlight(text, "elasticsearch search", 1, 300, true, null, null, null, null, null);
        assertThat(
            result,
            equalTo(
                List.of(
                    "<em>Elasticsearch</em> is a <em>search</em> engine built on Lucene. "
                        + "<em>Elasticsearch</em> supports distributed <em>search</em> across many nodes. "
                        + "<em>Elasticsearch</em> provides near real-time <em>search</em> capabilities. "
                        + "<em>Elasticsearch</em> scales horizontally for large <em>search</em> workloads."
                )
            )
        );
    }

    private List<String> process(Object fieldInput, String query, int numSnippets, int numWords) {
        return processWithHighlight(fieldInput, query, numSnippets, numWords, null, null, null, null, null, null);
    }

    private List<String> processMultivalueChunks(List<String> chunks, String query, int numSnippets, String order) {
        return processWithHighlight(chunks, query, numSnippets, 0, null, null, null, null, order, null);
    }

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

    private List<String> processWithHighlight(
        Object fieldInput,
        String query,
        int numSnippets,
        int numWords,
        Boolean highlight,
        String preTag,
        String postTag,
        String encoder,
        String order,
        String analyzer
    ) {
        MapExpression optionsMap = createOptions(numSnippets, numWords, highlight, preTag, postTag, encoder, order, analyzer);
        Expression expression = new TopSnippets(
            Source.EMPTY,
            field("field", DataType.KEYWORD),
            new Literal(Source.EMPTY, new BytesRef(query), DataType.KEYWORD),
            optionsMap
        );
        Object fieldValue;
        if (fieldInput instanceof List<?> chunks) {
            fieldValue = chunks.stream().map(c -> new BytesRef((String) c)).toList();
        } else {
            fieldValue = new BytesRef((String) fieldInput);
        }

        ExpressionEvaluator.Factory factory;
        if (analyzer != null) {
            Layout.Builder builder = new Layout.Builder();
            buildLayout(builder, expression);
            factory = EvalMapper.toEvaluator(
                FoldContext.small(),
                expression,
                builder.build(),
                org.elasticsearch.compute.lucene.EmptyIndexedByShardId.instance(),
                analysisRegistry
            );
        } else {
            factory = evaluator(expression);
        }

        try (ExpressionEvaluator eval = factory.get(driverContext(breakers)); Block block = eval.eval(row(List.of(fieldValue)))) {
            if (block.isNull(0)) {
                return null;
            }
            Object result = toJavaObject(block, 0);
            if (result instanceof BytesRef bytesRef) {
                return List.of(bytesRef.utf8ToString());
            } else {
                @SuppressWarnings("unchecked")
                List<BytesRef> list = (List<BytesRef>) result;
                return list.stream().map(BytesRef::utf8ToString).toList();
            }
        }
    }
}
