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
import org.elasticsearch.compute.operator.EvalOperator;
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
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.TopSnippets.DEFAULT_NUM_SNIPPETS;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.TopSnippets.DEFAULT_WORD_SIZE;
import static org.elasticsearch.xpack.esql.expression.function.scalar.util.ChunkUtils.chunkText;
import static org.hamcrest.Matchers.equalTo;

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
                    new TestCaseSupplier.TypedData(new BytesRef(query), DataType.KEYWORD, "query")
                ),
                "TopSnippetsEvaluator[field=Attribute[channel=0], query=Attribute[channel=1], "
                    + "chunkingSettings={\"strategy\":\"sentence\",\"max_chunk_size\":300,\"sentence_overlap\":0}, "
                    + "scorer=MemoryIndexChunkScorer, numSnippets=5]",
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
                    new TestCaseSupplier.TypedData(new BytesRef(query), DataType.KEYWORD, "query"),
                    new TestCaseSupplier.TypedData(createOptions(numSnippets, numWords), UNSUPPORTED, "options").forceLiteral()
                );

                return new TestCaseSupplier.TestCase(
                    values,
                    "TopSnippetsEvaluator[field=Attribute[channel=0], query=Attribute[channel=1], "
                        + "chunkingSettings={\"strategy\":\"sentence\",\"max_chunk_size\":25,\"sentence_overlap\":0}, "
                        + "scorer=MemoryIndexChunkScorer, numSnippets=3]",
                    DataType.KEYWORD,
                    equalTo(expectedResult)
                );
            }));
        }
        return result;
    }

    private static MapExpression createOptions(Integer numSnippets, Integer numWords) {
        List<Expression> optionsMap = new ArrayList<>();

        if (Objects.nonNull(numSnippets)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "num_snippets"));
            optionsMap.add(new Literal(Source.EMPTY, numSnippets, DataType.INTEGER));
        }

        if (Objects.nonNull(numWords)) {
            optionsMap.add(Literal.keyword(Source.EMPTY, "num_words"));
            optionsMap.add(new Literal(Source.EMPTY, numWords, DataType.INTEGER));
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

        assertNotNull("Should return results for matching query", result);
        assertFalse("Should have at least one result", result.isEmpty());

        assertTrue(
            "First snippet should be from the most relevant chunk (contains 'Elasticsearch' multiple times)",
            result.get(0).toLowerCase(Locale.ROOT).contains("elasticsearch")
                && (result.get(0).contains("powerful") || result.get(0).contains("supports") || result.get(0).contains("companies"))
        );
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

    private List<String> process(String str, String query, int numSnippets, int numWords) {
        MapExpression optionsMap = createOptions(numSnippets, numWords);

        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(
                new TopSnippets(Source.EMPTY, field("field", DataType.KEYWORD), field("query", DataType.KEYWORD), optionsMap)
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(str), new BytesRef(query))))
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
