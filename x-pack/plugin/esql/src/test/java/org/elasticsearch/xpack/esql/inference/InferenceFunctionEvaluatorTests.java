/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.inference.CompletionFunction;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRunner;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceFunctionEvaluatorTests extends ComputeTestCase {

    private ThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        this.threadPool = createThreadPool();
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    public void testFoldTextEmbeddingFunction() throws Exception {
        // Create a mock TextEmbedding function
        TextEmbedding textEmbeddingFunction = new TextEmbedding(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "test input"),
            Literal.keyword(Source.EMPTY, "test-model")
        );

        // Create a mock operator that returns a result
        Operator operator = mock(Operator.class);

        float[] embedding = randomEmbedding(between(1, 100));

        InferenceService inferenceService = mock(InferenceService.class);
        BulkInferenceRunner bulkInferenceRunner = mock(BulkInferenceRunner.class);

        doAnswer(i -> {
            threadPool.schedule(
                () -> i.getArgument(1, ActionListener.class).onResponse(List.of(inferenceResponse(embedding))),
                TimeValue.timeValueMillis(between(1, 10)),
                threadPool.generic()
            );

            return null;
        }).when(bulkInferenceRunner).executeBulk(any(), any());
        when(bulkInferenceRunner.threadPool()).thenReturn(threadPool);

        when(inferenceService.bulkInferenceRunner()).thenReturn(bulkInferenceRunner);

        when(operator.getOutput()).thenAnswer(i -> {
            FloatBlock.Builder outputBlockBuilder = blockFactory().newFloatBlockBuilder(1).beginPositionEntry();

            for (int j = 0; j < embedding.length; j++) {
                outputBlockBuilder.appendFloat(embedding[j]);
            }

            outputBlockBuilder.endPositionEntry();

            return new Page(outputBlockBuilder.build());
        });

        InferenceFunctionEvaluator.InferenceOperatorProvider inferenceOperatorProvider = (f, driverContext) -> operator;

        // Execute the fold operation
        InferenceFunctionEvaluator evaluator = InferenceFunctionEvaluator.factory().create(FoldContext.small(), inferenceService);

        AtomicReference<Expression> resultExpression = new AtomicReference<>();
        evaluator.fold(textEmbeddingFunction, ActionListener.wrap(resultExpression::set, ESTestCase::fail));

        assertBusy(() -> {
            assertNotNull(resultExpression.get());
            Literal result = as(resultExpression.get(), Literal.class);
            assertThat(result.dataType(), equalTo(DataType.DENSE_VECTOR));
            assertThat(as(result.value(), List.class).toArray(), equalTo(embedding));
        });

        // Check all breakers are empty after the operation is executed
        allBreakersEmpty();
    }

    public void testFoldTextEmbeddingFunctionWithNullInput() throws Exception {
        // Create a mock TextEmbedding function
        TextEmbedding textEmbeddingFunction = new TextEmbedding(Source.EMPTY, Literal.NULL, Literal.keyword(Source.EMPTY, "test-model"));

        // Create a mock operator that returns a result
        Operator operator = mock(Operator.class);

        Float[] embedding = randomArray(1, 100, Float[]::new, ESTestCase::randomFloat);

        when(operator.getOutput()).thenAnswer(i -> {
            FloatBlock.Builder outputBlockBuilder = blockFactory().newFloatBlockBuilder(1);
            outputBlockBuilder.appendNull();
            return new Page(outputBlockBuilder.build());
        });

        InferenceFunctionEvaluator.InferenceOperatorProvider inferenceOperatorProvider = (f, driverContext) -> operator;

        // Execute the fold operation
        InferenceFunctionEvaluator evaluator = new InferenceFunctionEvaluator(FoldContext.small(), inferenceOperatorProvider);

        AtomicReference<Expression> resultExpression = new AtomicReference<>();
        evaluator.fold(textEmbeddingFunction, ActionListener.wrap(resultExpression::set, ESTestCase::fail));

        assertBusy(() -> {
            assertNotNull(resultExpression.get());
            Literal result = as(resultExpression.get(), Literal.class);
            assertThat(result.dataType(), equalTo(DataType.NULL));
            assertThat(result.value(), nullValue());
        });

        // Check all breakers are empty after the operation is executed
        allBreakersEmpty();
    }

    public void testFoldWithNonFoldableFunction() {
        // A function with a non-literal argument is not foldable.
        TextEmbedding textEmbeddingFunction = new TextEmbedding(
            Source.EMPTY,
            mock(Attribute.class),
            Literal.keyword(Source.EMPTY, "test model")
        );

        InferenceFunctionEvaluator evaluator = new InferenceFunctionEvaluator(
            FoldContext.small(),
            (f, driverContext) -> mock(Operator.class)
        );

        AtomicReference<Exception> error = new AtomicReference<>();
        evaluator.fold(textEmbeddingFunction, ActionListener.wrap(r -> fail("should have failed"), error::set));

        assertNotNull(error.get());
        assertThat(error.get(), instanceOf(IllegalArgumentException.class));
        assertThat(error.get().getMessage(), equalTo("Inference function must be foldable"));
    }

    public void testFoldWithAsyncFailure() throws Exception {
        TextEmbedding textEmbeddingFunction = new TextEmbedding(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "test input"),
            Literal.keyword(Source.EMPTY, "test-model")
        );

        // Mock an operator that will trigger an async failure
        Operator operator = mock(Operator.class);
        doAnswer(invocation -> {
            // Simulate the operator finishing and then immediately calling the failure listener.
            // In that case getOutput() will replay the failure when called allowing us to catch the error.
            throw new RuntimeException("async failure");
        }).when(operator).getOutput();

        InferenceFunctionEvaluator.InferenceOperatorProvider inferenceOperatorProvider = (f, driverContext) -> operator;
        InferenceFunctionEvaluator evaluator = new InferenceFunctionEvaluator(FoldContext.small(), inferenceOperatorProvider);

        AtomicReference<Exception> error = new AtomicReference<>();
        evaluator.fold(textEmbeddingFunction, ActionListener.wrap(r -> fail("should have failed"), error::set));

        assertBusy(() -> assertNotNull(error.get()));
        assertThat(error.get(), instanceOf(RuntimeException.class));
        assertThat(error.get().getMessage(), equalTo("async failure"));

        allBreakersEmpty();
    }

    public void testFoldWithNullOutputPage() throws Exception {
        TextEmbedding textEmbeddingFunction = new TextEmbedding(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "test input"),
            Literal.keyword(Source.EMPTY, "test-model")
        );

        Operator operator = mock(Operator.class);
        when(operator.getOutput()).thenReturn(null);

        InferenceFunctionEvaluator.InferenceOperatorProvider inferenceOperatorProvider = (f, driverContext) -> operator;
        InferenceFunctionEvaluator evaluator = new InferenceFunctionEvaluator(FoldContext.small(), inferenceOperatorProvider);

        AtomicReference<Exception> error = new AtomicReference<>();
        evaluator.fold(textEmbeddingFunction, ActionListener.wrap(r -> fail("should have failed"), error::set));

        assertBusy(() -> assertNotNull(error.get()));
        assertThat(error.get(), instanceOf(IllegalStateException.class));
        assertThat(error.get().getMessage(), equalTo("Expected output page from inference operator"));

        allBreakersEmpty();
    }

    public void testFoldWithUnsupportedFunction() throws Exception {
        InferenceFunction<?> unsupported = mock(InferenceFunction.class);
        when(unsupported.foldable()).thenReturn(true);

        InferenceFunctionEvaluator evaluator = new InferenceFunctionEvaluator(FoldContext.small(), (f, driverContext) -> {
            throw new IllegalArgumentException("Unknown inference function: " + f.getClass().getName());
        });

        AtomicReference<Exception> error = new AtomicReference<>();
        evaluator.fold(unsupported, ActionListener.wrap(r -> fail("should have failed"), error::set));

        assertNotNull(error.get());
        assertThat(error.get(), instanceOf(IllegalArgumentException.class));
        assertThat(error.get().getMessage(), containsString("Unknown inference function"));

        allBreakersEmpty();
    }

    private float[] randomEmbedding(int length) {
        float[] embedding = new float[length];
        for (int i = 0; i < length; i++) {
            embedding[i] = randomFloat();
        }
        return embedding;
    }

    @SuppressWarnings("unchecked")
    public void testFoldCompletionFunction() throws Exception {
        // Create a mock CompletionFunction
        CompletionFunction completionFunction = new CompletionFunction(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "test prompt"),
            Literal.keyword(Source.EMPTY, "test-model")
        );

        // Create a mock operator that returns a result
        Operator operator = mock(Operator.class);

        String completionText = randomAlphaOfLengthBetween(10, 100);

        InferenceService inferenceService = mock(InferenceService.class);
        BulkInferenceRunner bulkInferenceRunner = mock(BulkInferenceRunner.class);

        doAnswer(i -> {
            threadPool.schedule(
                () -> i.getArgument(1, ActionListener.class).onResponse(List.of(completionResponse(completionText))),
                TimeValue.timeValueMillis(between(1, 10)),
                threadPool.generic()
            );

            return null;
        }).when(bulkInferenceRunner).executeBulk(any(), any());
        when(bulkInferenceRunner.threadPool()).thenReturn(threadPool);

        when(inferenceService.bulkInferenceRunner()).thenReturn(bulkInferenceRunner);

        when(operator.getOutput()).thenAnswer(i -> {
            BytesRefBlock.Builder outputBlockBuilder = blockFactory().newBytesRefBlockBuilder(1).beginPositionEntry();
            outputBlockBuilder.appendBytesRef(new org.apache.lucene.util.BytesRef(completionText));
            outputBlockBuilder.endPositionEntry();

            return new Page(outputBlockBuilder.build());
        });

        InferenceFunctionEvaluator.InferenceOperatorProvider inferenceOperatorProvider = (f, driverContext) -> operator;

        // Execute the fold operation
        InferenceFunctionEvaluator evaluator = InferenceFunctionEvaluator.factory().create(FoldContext.small(), inferenceService);

        AtomicReference<Expression> resultExpression = new AtomicReference<>();
        evaluator.fold(completionFunction, ActionListener.wrap(resultExpression::set, ESTestCase::fail));

        assertBusy(() -> {
            assertNotNull(resultExpression.get());
            Literal result = as(resultExpression.get(), Literal.class);
            assertThat(result.dataType(), equalTo(DataType.KEYWORD));
            assertThat(result.value(), instanceOf(BytesRef.class));
            BytesRef expectedBytesRef = new BytesRef(completionText);
            assertThat(result.value(), equalTo(expectedBytesRef));
        });

        // Check all breakers are empty after the operation is executed
        allBreakersEmpty();
    }

    public void testFoldCompletionFunctionWithNullInput() throws Exception {
        // Create a mock CompletionFunction with null prompt
        CompletionFunction completionFunction = new CompletionFunction(
            Source.EMPTY,
            Literal.NULL,
            Literal.keyword(Source.EMPTY, "test-model")
        );

        // Create a mock operator that returns a result
        Operator operator = mock(Operator.class);

        when(operator.getOutput()).thenAnswer(i -> {
            BytesRefBlock.Builder outputBlockBuilder = blockFactory().newBytesRefBlockBuilder(1);
            outputBlockBuilder.appendNull();
            return new Page(outputBlockBuilder.build());
        });

        InferenceFunctionEvaluator.InferenceOperatorProvider inferenceOperatorProvider = (f, driverContext) -> operator;

        // Execute the fold operation
        InferenceFunctionEvaluator evaluator = new InferenceFunctionEvaluator(FoldContext.small(), inferenceOperatorProvider);

        AtomicReference<Expression> resultExpression = new AtomicReference<>();
        evaluator.fold(completionFunction, ActionListener.wrap(resultExpression::set, ESTestCase::fail));

        assertBusy(() -> {
            assertNotNull(resultExpression.get());
            Literal result = as(resultExpression.get(), Literal.class);
            assertThat(result.dataType(), equalTo(DataType.NULL));
            assertThat(result.value(), nullValue());
        });

        // Check all breakers are empty after the operation is executed
        allBreakersEmpty();
    }

    public void testFoldCompletionFunctionWithNonFoldablePrompt() {
        // A function with a non-literal prompt is not foldable.
        CompletionFunction completionFunction = new CompletionFunction(
            Source.EMPTY,
            mock(Attribute.class),
            Literal.keyword(Source.EMPTY, "test model")
        );

        InferenceFunctionEvaluator evaluator = new InferenceFunctionEvaluator(
            FoldContext.small(),
            (f, driverContext) -> mock(Operator.class)
        );

        AtomicReference<Exception> error = new AtomicReference<>();
        evaluator.fold(completionFunction, ActionListener.wrap(r -> fail("should have failed"), error::set));

        assertNotNull(error.get());
        assertThat(error.get(), instanceOf(IllegalArgumentException.class));
        assertThat(error.get().getMessage(), equalTo("Inference function must be foldable"));
    }

    public void testFoldCompletionFunctionWithAsyncFailure() throws Exception {
        CompletionFunction completionFunction = new CompletionFunction(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "test prompt"),
            Literal.keyword(Source.EMPTY, "test-model")
        );

        // Mock an operator that will trigger an async failure
        Operator operator = mock(Operator.class);
        doAnswer(invocation -> {
            // Simulate the operator finishing and then immediately calling the failure listener.
            // In that case getOutput() will replay the failure when called allowing us to catch the error.
            throw new RuntimeException("async failure");
        }).when(operator).getOutput();

        InferenceFunctionEvaluator.InferenceOperatorProvider inferenceOperatorProvider = (f, driverContext) -> operator;
        InferenceFunctionEvaluator evaluator = new InferenceFunctionEvaluator(FoldContext.small(), inferenceOperatorProvider);

        AtomicReference<Exception> error = new AtomicReference<>();
        evaluator.fold(completionFunction, ActionListener.wrap(r -> fail("should have failed"), error::set));

        assertBusy(() -> assertNotNull(error.get()));
        assertThat(error.get(), instanceOf(RuntimeException.class));
        assertThat(error.get().getMessage(), equalTo("async failure"));

        allBreakersEmpty();
    }

    public void testFoldCompletionFunctionWithNullOutputPage() throws Exception {
        CompletionFunction completionFunction = new CompletionFunction(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "test prompt"),
            Literal.keyword(Source.EMPTY, "test-model")
        );

        Operator operator = mock(Operator.class);
        when(operator.getOutput()).thenReturn(null);

        InferenceFunctionEvaluator.InferenceOperatorProvider inferenceOperatorProvider = (f, driverContext) -> operator;
        InferenceFunctionEvaluator evaluator = new InferenceFunctionEvaluator(FoldContext.small(), inferenceOperatorProvider);

        AtomicReference<Exception> error = new AtomicReference<>();
        evaluator.fold(completionFunction, ActionListener.wrap(r -> fail("should have failed"), error::set));

        assertBusy(() -> assertNotNull(error.get()));
        assertThat(error.get(), instanceOf(IllegalStateException.class));
        assertThat(error.get().getMessage(), equalTo("Expected output page from inference operator"));

        allBreakersEmpty();
    }

    private InferenceAction.Response inferenceResponse(float[] embedding) {
        DenseEmbeddingFloatResults.Embedding embeddingResult = new DenseEmbeddingFloatResults.Embedding(embedding);
        return new InferenceAction.Response(new DenseEmbeddingFloatResults(List.of(embeddingResult)));
    }

    private InferenceAction.Response completionResponse(String completionText) {
        ChatCompletionResults.Result result = new ChatCompletionResults.Result(completionText);
        return new InferenceAction.Response(new ChatCompletionResults(List.of(result)));
    }
}
