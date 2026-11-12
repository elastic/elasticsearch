/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperatorTestCase;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RerankOperatorTests extends InferenceOperatorTestCase<RankedDocsResults> {
    private static final String SIMPLE_INFERENCE_ID = "test_rerank";
    private static final String DEFAULT_INFERENCE_ID = Rerank.DEFAULT_INFERENCE_ID;
    private static final String QUERY_TEXT = "test query";
    private static final int BATCH_SIZE = 20;
    private static final Exception NEVER_THROWN = new IllegalStateException("never thrown");

    private int inputChannel;
    private List<Integer> inputChannels;
    private int scoreChannel;

    /**
     * The endpoint for the tests shared by both RERANK operators. The default endpoint gets the batched
     * {@link RerankOperator} and any other endpoint gets the {@link SingleRequestRerankOperator}.
     */
    private String inferenceId;

    @Before
    public void initRerankChannels() {
        inputChannel = between(0, inputsCount - 1);
        inputChannels = List.of(inputChannel);
        // Score channel can be anywhere in the page
        scoreChannel = between(0, inputsCount);
        inferenceId = randomFrom(SIMPLE_INFERENCE_ID, DEFAULT_INFERENCE_ID);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new RerankOperator.Factory(
            mockedInferenceService(),
            inferenceId,
            QUERY_TEXT,
            inputChannels.stream().map(this::evaluatorFactory).toList(),
            scoreChannel,
            BATCH_SIZE,
            null
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results, hasSize(input.size()));

        for (int curPage = 0; curPage < input.size(); curPage++) {
            Page inputPage = input.get(curPage);
            Page resultPage = results.get(curPage);

            assertEquals(inputPage.getPositionCount(), resultPage.getPositionCount());

            // If scoreChannel is at the end, the score block is appended (+1 block)
            // Otherwise, it replaces an existing block (same block count)
            int expectedBlockCount = scoreChannel == inputPage.getBlockCount() ? inputPage.getBlockCount() + 1 : inputPage.getBlockCount();
            assertEquals(expectedBlockCount, resultPage.getBlockCount());

            // Verify all original blocks are preserved (except the one replaced by the score block)
            for (int channel = 0; channel < inputPage.getBlockCount(); channel++) {
                if (channel != scoreChannel) {
                    Block inputBlock = inputPage.getBlock(channel);
                    Block resultBlock = resultPage.getBlock(channel);
                    assertBlockContentEquals(inputBlock, resultBlock);
                }
            }

            // Verify rerank scores in the score channel
            assertRerankResults(inputPage, resultPage);
        }
    }

    private void assertRerankResults(Page inputPage, Page resultPage) {
        DoubleBlock scoreBlock = resultPage.getBlock(scoreChannel);

        for (int curPos = 0; curPos < inputPage.getPositionCount(); curPos++) {
            // Collect all non-empty text values from all input channels at this position
            List<String> inputTexts = new ArrayList<>();
            boolean allNull = true;

            for (int channel : inputChannels) {
                BytesRefBlock inputBlock = inputPage.getBlock(channel);
                if (inputBlock.isNull(curPos) == false) {
                    allNull = false;
                    inputTexts.addAll(readNonEmptyInputs(inputBlock, curPos));
                }
            }

            if (allNull || inputTexts.isEmpty()) {
                // All null or empty/whitespace-only inputs should produce null scores
                assertThat(scoreBlock.isNull(curPos), equalTo(true));
            } else {
                // Verify score is present
                assertFalse(scoreBlock.isNull(curPos));
                double score = scoreBlock.getDouble(scoreBlock.getFirstValueIndex(curPos));

                // For multi-valued positions, the score should be the max of all input scores
                // (see RerankOutputBuilder.appendResponseToBlock)
                double expectedScore = inputTexts.stream().mapToDouble(this::makeScore).max().orElseThrow();

                assertThat(score, closeTo(expectedScore, 0.0001));
            }
        }
    }

    /**
     * Reads all non-empty text values from a position, matching the filtering done by RerankRequestIterator.
     */
    private List<String> readNonEmptyInputs(BytesRefBlock block, int pos) {
        List<String> inputs = new ArrayList<>();
        int valueCount = block.getValueCount(pos);
        int firstValueIndex = block.getFirstValueIndex(pos);
        BytesRef scratch = new BytesRef();

        for (int i = 0; i < valueCount; i++) {
            scratch = block.getBytesRef(firstValueIndex + i, scratch);
            String text = scratch.utf8ToString();
            // Match the filtering logic in RerankRequestIterator.readInputText
            if (Strings.hasText(text)) {
                inputs.add(text);
            }
        }

        return inputs;
    }

    /**
     * Tests reranking with multiple input fields. Values from all input channels
     * are combined and sent to the inference service.
     */
    public void testMultipleInputFields() throws Exception {
        // Use two different input channels
        int channel1 = 0;
        int channel2 = 1;

        // Save original channels and set up multi-field
        List<Integer> originalChannels = inputChannels;
        inputChannels = List.of(channel1, channel2);

        try (
            BytesRefBlock.Builder blockBuilder1 = blockFactory().newBytesRefBlockBuilder(3);
            BytesRefBlock.Builder blockBuilder2 = blockFactory().newBytesRefBlockBuilder(3)
        ) {
            // Block 1:
            // Position 0: single value "a"
            blockBuilder1.appendBytesRef(new BytesRef("a"));
            // Position 1: two values ["b1", "b2"]
            blockBuilder1.beginPositionEntry();
            blockBuilder1.appendBytesRef(new BytesRef("b1"));
            blockBuilder1.appendBytesRef(new BytesRef("b2"));
            blockBuilder1.endPositionEntry();
            // Position 2: null
            blockBuilder1.appendNull();
            // Position 3: single value
            blockBuilder1.appendBytesRef(new BytesRef("c"));

            // Block 2:
            // Position 0: two values ["x1", "x2"]
            blockBuilder2.beginPositionEntry();
            blockBuilder2.appendBytesRef(new BytesRef("x1"));
            blockBuilder2.appendBytesRef(new BytesRef("x2"));
            blockBuilder2.endPositionEntry();
            // Position 1: single value "y"
            blockBuilder2.appendBytesRef(new BytesRef("y"));
            // Position 2: null
            blockBuilder2.appendNull();
            // Position 3: null
            blockBuilder2.appendNull();

            var runner = new TestDriverRunner().builder(driverContext()).input(blockBuilder1.build(), blockBuilder2.build());

            try {
                // Create a simple factory with multiple input evaluators
                Operator.OperatorFactory factory = new RerankOperator.Factory(
                    mockedInferenceService(),
                    inferenceId,
                    QUERY_TEXT,
                    inputChannels.stream().map(this::evaluatorFactory).toList(),
                    2,
                    BATCH_SIZE,
                    null
                );

                // Verify factory is created correctly
                assertNotNull(factory);

                List<Page> results = runner.run(factory);
                assertThat(results, hasSize(1));
                Page resultPage = results.get(0);

                assertThat(resultPage.getPositionCount(), equalTo(4));
                assertThat(resultPage.getBlockCount(), equalTo(3)); // original + score

                // get the score channel
                DoubleBlock scoreBlock = resultPage.getBlock(2);

                // Position 0
                assertFalse(scoreBlock.isNull(0));
                double expectedScore0 = makeScore("a", "x1", "x2");
                assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(0)), closeTo(expectedScore0, 0.0001));

                // Position 1
                assertFalse(scoreBlock.isNull(1));
                double expectedScore1 = makeScore("b1", "b2", "y");
                assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(1)), closeTo(expectedScore1, 0.0001));

                // Position 2: null - should produce null score
                assertTrue(scoreBlock.isNull(2));

                // Position 3
                assertFalse(scoreBlock.isNull(3));
                double expectedScore3 = makeScore("c");
                assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(3)), closeTo(expectedScore3, 0.0001));

            } finally {
                // Restore original channels
                inputChannels = originalChannels;
            }
        }

        allBreakersEmpty();
    }

    /**
     * Tests reranking with multi-valued fields. When a position has multiple values,
     * all values are sent to the inference service, and the max score is returned.
     */
    public void testMultiValuedFields() throws Exception {
        final int positionCount = 4;

        // Create a page with multi-valued BytesRef blocks
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(positionCount)) {
            // Position 0: single value
            builder.appendBytesRef(new BytesRef("single_value"));

            // Position 1: two values - should get max score
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("multi_a"));
            builder.appendBytesRef(new BytesRef("multi_b"));
            builder.endPositionEntry();

            // Position 2: null
            builder.appendNull();

            // Position 3: three values - should get max score
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("triple_x"));
            builder.appendBytesRef(new BytesRef("triple_y"));
            builder.appendBytesRef(new BytesRef("triple_z"));
            builder.endPositionEntry();

            var runner = new TestDriverRunner().builder(driverContext()).input(builder.build());

            // Create operator with a single input channel (channel 0)
            List<Integer> savedChannels = inputChannels;
            inputChannels = List.of(0);
            int savedScoreChannel = scoreChannel;
            scoreChannel = 1; // Append score as new column

            try {
                Operator.OperatorFactory factory = new RerankOperator.Factory(
                    mockedInferenceService(),
                    inferenceId,
                    QUERY_TEXT,
                    inputChannels.stream().map(this::evaluatorFactory).toList(),
                    scoreChannel,
                    BATCH_SIZE,
                    null
                );

                List<Page> results = runner.run(factory);
                assertThat(results, hasSize(1));
                Page resultPage = results.get(0);

                assertThat(resultPage.getPositionCount(), equalTo(positionCount));
                assertThat(resultPage.getBlockCount(), equalTo(2)); // original + score

                DoubleBlock scoreBlock = resultPage.getBlock(scoreChannel);

                // Position 0: single value - score based on "single_value"
                assertFalse(scoreBlock.isNull(0));
                double expectedScore0 = makeScore("single_value");
                assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(0)), closeTo(expectedScore0, 0.0001));

                // Position 1: multi-valued - max score of "multi_a" and "multi_b"
                assertFalse(scoreBlock.isNull(1));
                double expectedScore1 = makeScore("multi_a", "multi_b");
                assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(1)), closeTo(expectedScore1, 0.0001));

                // Position 2: null - should produce null score
                assertTrue(scoreBlock.isNull(2));

                // Position 3: multi-valued - max score of three values
                assertFalse(scoreBlock.isNull(3));
                double expectedScore3 = makeScore("triple_x", "triple_y", "triple_z");
                assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(3)), closeTo(expectedScore3, 0.0001));

                // Clean up result pages
                results.forEach(Page::releaseBlocks);
            } finally {
                inputChannels = savedChannels;
                scoreChannel = savedScoreChannel;
            }
        }
    }

    public void testListwiseScoresComeFromOneRequestOnSinglePage() {
        AtomicInteger requestCount = new AtomicInteger();
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(docsPage(runner.blockFactory(), docRange(1, 100)));

        List<Page> results = runner.run(rerankFactory(listwiseInferenceService(requestCount)));
        try {
            assertThat(requestCount.get(), equalTo(1));
            assertThat(results.stream().mapToInt(Page::getPositionCount).sum(), equalTo(100));
            assertListwiseScores(results);
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(results.iterator(), p -> p::releaseBlocks)));
        }
    }

    public void testListwiseScoresComeFromOneRequestAcrossPages() {
        AtomicInteger requestCount = new AtomicInteger();
        var runner = new TestDriverRunner().builder(driverContext());
        BlockFactory blockFactory = runner.blockFactory();
        runner.input(
            docsPage(blockFactory, docRange(1, 32)),
            docsPage(blockFactory, docRange(33, 64)),
            docsPage(blockFactory, docRange(65, 96)),
            docsPage(blockFactory, docRange(97, 100))
        );

        List<Page> results = runner.run(rerankFactory(listwiseInferenceService(requestCount)));
        try {
            assertThat(requestCount.get(), equalTo(1));
            assertThat(results.stream().map(Page::getPositionCount).toList(), equalTo(List.of(32, 32, 32, 4)));
            assertListwiseScores(results);
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(results.iterator(), p -> p::releaseBlocks)));
        }
    }

    public void testOneRequestAcrossPagesWithMultiValuedAndEmptyRows() {
        AtomicInteger requestCount = new AtomicInteger();
        var runner = new TestDriverRunner().builder(driverContext());
        BlockFactory blockFactory = runner.blockFactory();
        runner.input(
            docsPage(blockFactory, docWithRelevance(1), null, List.of(docWithRelevance(2), docWithRelevance(3))),
            docsPage(blockFactory, docWithRelevance(4), "  ")
        );

        List<Page> results = runner.run(rerankFactory(listwiseInferenceService(requestCount)));
        try {
            assertThat(requestCount.get(), equalTo(1));
            assertThat(results, hasSize(2));

            double total = 1 + 2 + 3 + 4;
            DoubleBlock firstPageScores = results.get(0).getBlock(1);
            assertThat(firstPageScores.getDouble(firstPageScores.getFirstValueIndex(0)), closeTo(1 / total, 0.0001));
            assertTrue(firstPageScores.isNull(1));
            assertThat(firstPageScores.getDouble(firstPageScores.getFirstValueIndex(2)), closeTo(3 / total, 0.0001));

            DoubleBlock secondPageScores = results.get(1).getBlock(1);
            assertThat(secondPageScores.getDouble(secondPageScores.getFirstValueIndex(0)), closeTo(4 / total, 0.0001));
            assertTrue(secondPageScores.isNull(1));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(results.iterator(), p -> p::releaseBlocks)));
        }
    }

    public void testEndpointReturningFewerScoresThanDocuments() {
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(docsPage(runner.blockFactory(), docRange(1, 20)));
        InferenceService topNService = mockedInferenceService(inputs -> false, NEVER_THROWN, null, keepTopN(5, this::mockInferenceResult));

        Exception e = expectThrows(IllegalArgumentException.class, () -> runner.run(rerankFactory(topNService)));
        assertThat(
            e.getMessage(),
            equalTo(
                "Inference endpoint [test_rerank] returned scores for [5] of the [20] documents sent by RERANK. "
                    + "If the endpoint is configured with [top_n], set it to at least the number of documents RERANK receives, "
                    + "or reduce that number with LIMIT."
            )
        );
    }

    public void testDefaultEndpointKeepsBatchedRequests() {
        AtomicInteger requestCount = new AtomicInteger();
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(docsPage(runner.blockFactory(), docRange(1, 100)));
        InferenceService countingService = mockedInferenceService(inputs -> false, NEVER_THROWN, requestCount);

        List<Page> results = runner.run(rerankFactory(DEFAULT_INFERENCE_ID, countingService));
        try {
            assertThat(requestCount.get(), equalTo(100 / BATCH_SIZE));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(results.iterator(), p -> p::releaseBlocks)));
        }
    }

    public void testNoRequestWhenNoRowHasText() {
        AtomicInteger requestCount = new AtomicInteger();
        var runner = new TestDriverRunner().builder(driverContext());
        BlockFactory blockFactory = runner.blockFactory();
        runner.input(docsPage(blockFactory, null, "  "), docsPage(blockFactory, ""));

        List<Page> results = runner.run(rerankFactory(listwiseInferenceService(requestCount)));
        try {
            assertThat(requestCount.get(), equalTo(0));
            assertThat(results.stream().map(Page::getPositionCount).toList(), equalTo(List.of(2, 1)));
            for (Page page : results) {
                DoubleBlock scores = page.getBlock(1);
                for (int pos = 0; pos < page.getPositionCount(); pos++) {
                    assertTrue(scores.isNull(pos));
                }
            }
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(results.iterator(), p -> p::releaseBlocks)));
        }
    }

    public void testCloseBeforeFinishSendsNoRequest() {
        AtomicInteger requestCount = new AtomicInteger();
        DriverContext driverContext = driverContext();
        try (Operator operator = rerankFactory(listwiseInferenceService(requestCount)).get(driverContext)) {
            operator.addInput(docsPage(driverContext.blockFactory(), docRange(1, 10)));
            assertTrue(operator.needsInput());
        }
        assertThat(requestCount.get(), equalTo(0));
    }

    private Operator.OperatorFactory rerankFactory(InferenceService inferenceService) {
        return rerankFactory(SIMPLE_INFERENCE_ID, inferenceService);
    }

    private Operator.OperatorFactory rerankFactory(String inferenceId, InferenceService inferenceService) {
        return new RerankOperator.Factory(inferenceService, inferenceId, QUERY_TEXT, List.of(evaluatorFactory(0)), 1, BATCH_SIZE, null);
    }

    private InferenceService listwiseInferenceService(AtomicInteger requestCount) {
        return mockedInferenceService(inputs -> false, NEVER_THROWN, requestCount, RerankOperatorTests::listwiseScores);
    }

    /**
     * Scores like a listwise reranker: a document's score is its relevance (see {@link #docWithRelevance}) divided by the
     * total relevance of every document in the same request, so scores depend on which other documents share the request.
     */
    private static RankedDocsResults listwiseScores(InferenceAction.Request request) {
        List<String> inputs = request.getInput();
        double totalRelevance = inputs.stream().mapToInt(RerankOperatorTests::relevanceOf).sum();
        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            String input = inputs.get(i);
            rankedDocs.add(new RankedDocsResults.RankedDoc(i, (float) (relevanceOf(input) / totalRelevance), input));
        }
        return new RankedDocsResults(rankedDocs);
    }

    /**
     * Wraps {@code scorer} so that only its {@code n} best-scoring documents are returned, like an endpoint configured
     * with {@code top_n}.
     */
    private static Function<InferenceAction.Request, RankedDocsResults> keepTopN(
        int n,
        Function<InferenceAction.Request, RankedDocsResults> scorer
    ) {
        return request -> new RankedDocsResults(
            scorer.apply(request)
                .getRankedDocs()
                .stream()
                .sorted(Comparator.comparingDouble(RankedDocsResults.RankedDoc::relevanceScore).reversed())
                .limit(n)
                .toList()
        );
    }

    /**
     * Asserts every row was scored against all rows in {@code results}, as {@link #listwiseScores} does when every
     * document is in one request. Expects one document per row.
     */
    private void assertListwiseScores(List<Page> results) {
        BytesRef scratch = new BytesRef();
        int totalRelevance = 0;
        for (Page page : results) {
            BytesRefBlock docs = page.getBlock(0);
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                totalRelevance += relevanceOf(docs.getBytesRef(docs.getFirstValueIndex(pos), scratch).utf8ToString());
            }
        }
        for (Page page : results) {
            BytesRefBlock docs = page.getBlock(0);
            DoubleBlock scores = page.getBlock(1);
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                String doc = docs.getBytesRef(docs.getFirstValueIndex(pos), scratch).utf8ToString();
                double expected = (double) relevanceOf(doc) / totalRelevance;
                assertThat(doc, scores.getDouble(scores.getFirstValueIndex(pos)), closeTo(expected, 0.0001));
            }
        }
    }

    private static Object[] docRange(int fromInclusive, int toInclusive) {
        return IntStream.rangeClosed(fromInclusive, toInclusive).mapToObj(RerankOperatorTests::docWithRelevance).toArray();
    }

    private static String docWithRelevance(int relevance) {
        return "doc_" + relevance;
    }

    // Only accepts texts built by docWithRelevance
    private static int relevanceOf(String doc) {
        return Integer.parseInt(doc.substring("doc_".length()));
    }

    /**
     * Builds a single-column page where each value is a {@link String}, {@code null}, or a {@link List} of strings for a
     * multi-valued position.
     */
    private static Page docsPage(BlockFactory blockFactory, Object... values) {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(values.length)) {
            for (Object value : values) {
                switch (value) {
                    case null -> builder.appendNull();
                    case String text -> builder.appendBytesRef(new BytesRef(text));
                    case List<?> texts -> {
                        builder.beginPositionEntry();
                        for (Object text : texts) {
                            builder.appendBytesRef(new BytesRef((String) text));
                        }
                        builder.endPositionEntry();
                    }
                    default -> throw new IllegalArgumentException("unsupported value [" + value + "]");
                }
            }
            return new Page(builder.build());
        }
    }

    @Override
    protected RankedDocsResults mockInferenceResult(InferenceAction.Request request) {
        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>();
        List<String> inputs = request.getInput();

        for (int i = 0; i < inputs.size(); i++) {
            String input = inputs.get(i);
            rankedDocs.add(new RankedDocsResults.RankedDoc(i, makeScore(input), input));
        }

        return new RankedDocsResults(rankedDocs);
    }

    private float makeScore(String... textValues) {
        float maxHash = Integer.MIN_VALUE;
        for (String text : textValues) {
            if (Strings.hasText(text)) {
                int hash = text.hashCode();
                if (hash > maxHash) {
                    maxHash = hash;
                }
            }
        }

        return maxHash / Integer.MAX_VALUE;
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "RerankOperator[inference_id=[" + inferenceId + "], query=[" + QUERY_TEXT + "], score_channel=[" + scoreChannel + "]]"
        );
    }

    public void testInferenceFailure() {
        AtomicBoolean shouldFail = new AtomicBoolean(true);
        Exception expectedException = new ElasticsearchException("Inference service unavailable");
        InferenceService failingService = mockedInferenceService(shouldFail, expectedException);

        Operator.OperatorFactory factory = new RerankOperator.Factory(
            failingService,
            inferenceId,
            QUERY_TEXT,
            List.of(evaluatorFactory(inputChannel)),
            scoreChannel,
            BATCH_SIZE,
            null
        );

        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(ensureNonNullPages(simpleInput(runner.context().blockFactory(), between(1, 100))));
        Exception actualException = expectThrows(ElasticsearchException.class, () -> runner.run(factory));

        assertThat(actualException.getMessage(), equalTo("Inference service unavailable"));
    }

    private List<Page> ensureNonNullPages(SourceOperator input) {
        return CannedSourceOperator.collectPages(input).stream().map(this::ensureNonNullPage).toList();
    }

    /**
     * Replaces the rerank input channel block of the given page with non-empty text values when it is entirely null,
     * so that an inference request is always dispatched for the page.
     */
    private Page ensureNonNullPage(Page page) {
        BytesRefBlock inputBlock = page.getBlock(inputChannel);
        for (int pos = 0; pos < inputBlock.getPositionCount(); pos++) {
            if (inputBlock.isNull(pos) == false) {
                return page;
            }
        }

        Block[] blocks = new Block[page.getBlockCount()];
        for (int b = 0; b < blocks.length; b++) {
            blocks[b] = page.getBlock(b);
        }
        try (BytesRefBlock.Builder builder = inputBlock.blockFactory().newBytesRefBlockBuilder(page.getPositionCount())) {
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                builder.appendBytesRef(new BytesRef(randomAlphaOfLength(10)));
            }
            blocks[inputChannel] = builder.build();
        }
        inputBlock.close();
        return new Page(blocks);
    }
}
