/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.core.common.chunks.MemoryIndexChunkScorer;
import org.elasticsearch.xpack.core.common.chunks.ScoredChunk;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.TopSnippets.DEFAULT_NUM_SNIPPETS;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.TopSnippets.DEFAULT_WORD_SIZE;
import static org.elasticsearch.xpack.esql.expression.function.scalar.util.ChunkUtils.chunkText;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TopSnippetsTests extends AbstractScalarFunctionTestCase {

    private static final String PARAGRAPH_INPUT = """
        The Adirondacks, a vast mountain region in northern New York, offer a breathtaking mix of rugged wilderness, serene lakes,
        and charming small towns. Spanning over six million acres, the Adirondack Park is larger than Yellowstone, Yosemite, and the
        Grand Canyon combined, yet it's dotted with communities where people live, work, and play amidst nature. Visitors come year-round
        to hike High Peaks trails, paddle across mirror-like waters, or ski through snow-covered forests. The area's pristine beauty,
        rich history, and commitment to conservation create a unique balance between wild preservation and human presence, making
        the Adirondacks a timeless escape into the tranquility of nature.
        """;

    public TopSnippetsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    private static String randomWordsBetween(int min, int max) {
        return IntStream.range(0, randomIntBetween(min, max))
            .mapToObj(i -> randomAlphaOfLengthBetween(1, 10))
            .collect(Collectors.joining(" "));
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(testCaseSuppliers());
    }

    private static List<TestCaseSupplier> testCaseSuppliers() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(createTestCaseSupplier("TopSnippets with defaults", DataType.KEYWORD, DataType.KEYWORD));
        suppliers.add(createTestCaseSupplier("TopSnippets with defaults text input", DataType.TEXT, DataType.KEYWORD));
        return addFunctionNamedParams(suppliers);
    }

    private static TestCaseSupplier createTestCaseSupplier(String description, DataType fieldDataType, DataType queryDataType) {
        return new TestCaseSupplier(description, List.of(fieldDataType, queryDataType), () -> {
            String text = randomWordsBetween(25, 50);
            String query = randomFrom("park", "nature", "trail");
            ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(DEFAULT_WORD_SIZE, 0);

            List<String> chunks = chunkText(text, chunkingSettings);
            MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();
            List<String> scoredChunks = scorer.scoreChunks(chunks, query, DEFAULT_NUM_SNIPPETS, false)
                .stream()
                .map(ScoredChunk::content)
                .toList();

            Object expectedResult;
            if (scoredChunks.isEmpty()) {
                expectedResult = null;
            } else if (scoredChunks.size() == 1) {
                expectedResult = new BytesRef(scoredChunks.get(0).trim());
            } else {
                expectedResult = scoredChunks.stream().map(s -> new BytesRef(s.trim())).toList();
            }

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), fieldDataType, "field"),
                    new TestCaseSupplier.TypedData(new BytesRef(query), DataType.KEYWORD, "query").forceLiteral()
                ),
                "TopSnippetsEvaluator[field=Attribute[channel=0], queryString="
                    + query
                    + ", "
                    + "chunkingSettings={\"strategy\":\"sentence\",\"max_chunk_size\":300,\"sentence_overlap\":0}, "
                    + "scorer=MemoryIndexChunkScorer, numSnippets=5, docOrder=false]",
                DataType.KEYWORD,
                equalTo(expectedResult)
            );
        });
    }

    /**
     * Adds function named parameters to all the test case suppliers provided
     */
    private static List<TestCaseSupplier> addFunctionNamedParams(List<TestCaseSupplier> suppliers) {
        List<TestCaseSupplier> result = new ArrayList<>(suppliers);
        for (TestCaseSupplier supplier : suppliers) {
            List<DataType> dataTypes = new ArrayList<>(supplier.types());
            dataTypes.add(UNSUPPORTED);
            result.add(new TestCaseSupplier(supplier.name() + ", with options", dataTypes, () -> {
                String text = randomWordsBetween(25, 50);
                String query = randomFrom("park", "nature", "trail");
                int numSnippets = 3;
                int numWords = 25;
                ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(numWords, 0);

                List<String> chunks = chunkText(text, chunkingSettings);
                MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();
                List<String> scoredChunks = scorer.scoreChunks(chunks, query, numSnippets, false)
                    .stream()
                    .map(ScoredChunk::content)
                    .toList();

                Object expectedResult;
                if (scoredChunks.isEmpty()) {
                    expectedResult = null;
                } else if (scoredChunks.size() == 1) {
                    expectedResult = new BytesRef(scoredChunks.get(0).trim());
                } else {
                    expectedResult = scoredChunks.stream().map(s -> new BytesRef(s.trim())).toList();
                }

                List<TestCaseSupplier.TypedData> values = List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), supplier.types().get(0), "field"),
                    new TestCaseSupplier.TypedData(new BytesRef(query), DataType.KEYWORD, "query").forceLiteral(),
                    new TestCaseSupplier.TypedData(createOptions(numSnippets, numWords), UNSUPPORTED, "options").forceLiteral()
                );

                return new TestCaseSupplier.TestCase(
                    values,
                    "TopSnippetsEvaluator[field=Attribute[channel=0], queryString="
                        + query
                        + ", "
                        + "chunkingSettings={\"strategy\":\"sentence\",\"max_chunk_size\":25,\"sentence_overlap\":0}, "
                        + "scorer=MemoryIndexChunkScorer, numSnippets=3, docOrder=false]",
                    DataType.KEYWORD,
                    equalTo(expectedResult)
                );
            }));
        }
        return result;
    }

    private static MapExpression createOptions(Integer numSnippets, Integer numWords) {
        return createOptions(numSnippets, numWords, null, null, null, null, null);
    }

    private static MapExpression createOptions(
        Integer numSnippets,
        Integer numWords,
        Boolean highlight,
        String preTag,
        String postTag,
        String encoder,
        String order
    ) {
        List<Expression> optionsMap = new ArrayList<>();

        if (Objects.nonNull(numSnippets)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "num_snippets"));
            optionsMap.add(new Literal(Source.EMPTY, numSnippets, DataType.INTEGER));
        }

        if (Objects.nonNull(numWords)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "num_words"));
            optionsMap.add(new Literal(Source.EMPTY, numWords, DataType.INTEGER));
        }

        if (Objects.nonNull(highlight)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "highlight"));
            optionsMap.add(new Literal(Source.EMPTY, highlight, DataType.BOOLEAN));
        }

        if (Objects.nonNull(preTag)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "pre_tag"));
            optionsMap.add(Literal.keyword(Source.EMPTY, preTag));
        }

        if (Objects.nonNull(postTag)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "post_tag"));
            optionsMap.add(Literal.keyword(Source.EMPTY, postTag));
        }

        if (Objects.nonNull(encoder)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "encoder"));
            optionsMap.add(Literal.keyword(Source.EMPTY, encoder));
        }

        if (Objects.nonNull(order)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "order"));
            optionsMap.add(Literal.keyword(Source.EMPTY, order));
        }

        return optionsMap.isEmpty() ? null : new MapExpression(Source.EMPTY, optionsMap);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Expression options = args.size() < 3 ? null : args.get(2);
        return new TopSnippets(source, args.get(0), args.get(1), options);
    }

    @Override
    public void testFold() {
        Expression expression = buildFieldExpression(testCase);
        // Skip testFold if the expression is not foldable (e.g., when options contains MapExpression)
        if (expression.foldable() == false) {
            return;
        }
        super.testFold();
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
        List<String> result = processWithHighlight(text, "park", 5, 300, true, null, null, null, null);
        assertThat(result, equalTo(List.of("The Adirondack <em>Park</em> is a beautiful wilderness area.")));
    }

    public void testHighlightCustomTags() {
        String text = "The Adirondack Park is a beautiful wilderness area.";
        List<String> result = processWithHighlight(text, "park", 5, 300, true, "<b>", "</b>", null, null);
        assertThat(result, equalTo(List.of("The Adirondack <b>Park</b> is a beautiful wilderness area.")));
    }

    public void testHighlightMultipleTerms() {
        String text = "Elasticsearch is a search engine. Lucene powers Elasticsearch.";
        List<String> result = processWithHighlight(text, "elasticsearch lucene", 5, 300, true, null, null, null, null);
        assertThat(result, equalTo(List.of("<em>Elasticsearch</em> is a search engine. <em>Lucene</em> powers <em>Elasticsearch</em>.")));
    }

    public void testHighlightFalseReturnsPlainText() {
        String text = "The Adirondack Park is a beautiful wilderness area.";
        List<String> withHighlight = processWithHighlight(text, "park", 5, 300, true, null, null, null, null);
        List<String> withoutHighlight = processWithHighlight(text, "park", 5, 300, false, null, null, null, null);
        List<String> noHighlightOption = process(text, "park", 5, 300);

        assertThat(withHighlight, equalTo(List.of("The Adirondack <em>Park</em> is a beautiful wilderness area.")));
        assertThat(withoutHighlight, equalTo(List.of("The Adirondack Park is a beautiful wilderness area.")));
        assertThat(noHighlightOption, equalTo(List.of("The Adirondack Park is a beautiful wilderness area.")));
    }

    public void testHighlightHtmlEncoder() {
        String text = "Use <b>bold</b> & special chars with the Ring.";
        List<String> result = processWithHighlight(text, "ring", 5, 300, true, null, null, "html", null);
        assertThat(result, equalTo(List.of("Use &lt;b&gt;bold&lt;&#x2F;b&gt; &amp; special chars with the <em>Ring</em>.")));
    }

    /**
     * TOP_SNIPPETS uses StandardAnalyzer (no stemming), so query "return" matches only the exact token "return",
     * not "returns". The search highlight API instead uses the field's index analyzer from the mapping
     * (see DefaultHighlighter.buildHighlighter / getIndexAnalyzer), so when a field is mapped with e.g. the
     * english analyzer, the highlight API will highlight "returns" for query "return". Parity would require
     * passing the field's analyzer when the input is from an indexed field.
     */
    public void testHighlightNoStemmingUsesExactTermOnly() {
        String text = "The API returns results. Use the return value to continue.";
        List<String> result = processWithHighlight(text, "return", 5, 300, true, null, null, null, null);
        assertThat(result, equalTo(List.of("The API returns results. Use the <em>return</em> value to continue.")));
    }

    public void testHighlightPreservesWholeChunk() {
        // A long chunk with many matching terms — highlighting must return the entire chunk as-is
        // (with markup), not split it into smaller passages.
        String text = "Elasticsearch is a search engine built on Lucene. "
            + "Elasticsearch supports distributed search across many nodes. "
            + "Elasticsearch provides near real-time search capabilities. "
            + "Elasticsearch scales horizontally for large search workloads.";
        List<String> result = processWithHighlight(text, "elasticsearch search", 1, 300, true, null, null, null, null);
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

    private List<String> process(String str, String query, int numSnippets, int numWords) {
        return processWithHighlight(str, query, numSnippets, numWords, null, null, null, null, null);
    }

    private List<String> processMultivalueChunks(List<String> chunks, String query, int numSnippets, String order) {
        return processWithHighlight(chunks, query, numSnippets, 0, null, null, null, null, order);
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
        String order
    ) {
        MapExpression optionsMap = createOptions(numSnippets, numWords, highlight, preTag, postTag, encoder, order);
        Object fieldValue;
        if (fieldInput instanceof List<?> chunks) {
            fieldValue = chunks.stream().map(c -> new BytesRef((String) c)).toList();
        } else {
            fieldValue = new BytesRef((String) fieldInput);
        }

        try (
            ExpressionEvaluator eval = evaluator(
                new TopSnippets(
                    Source.EMPTY,
                    field("field", DataType.KEYWORD),
                    new Literal(Source.EMPTY, new BytesRef(query), DataType.KEYWORD),
                    optionsMap
                )
            ).get(driverContext());
            Block block = eval.eval(row(List.of(fieldValue)))
        ) {
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
