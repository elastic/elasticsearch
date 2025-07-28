/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRunner;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base test class for LogicalPlanPreOptimizer tests.
 * <p>
 * Provides common infrastructure for testing pre-optimization rules, including:
 * - Thread pool management for async operations
 * - inference model implementations for inference function testing
 * - Mock services and runners for inference execution
 * - Helper methods for plan creation and manipulation
 */
public class AbstractLogicalPlanPreOptimizerTests extends ESTestCase {

    //
    // Embedding model types and implementations
    //

    /**
     * Available textembedding model types for testing.
     */
    public enum TestEmbeddingModel {
        FLOAT_EMBEDDING_MODEL,
        BYTES_EMBEDDING_MODEL,
        BITS_EMBEDDING_MODEL
    }

    /**
     * Interface for embedding model implementations.
     */
    protected interface TextEmbeddingModelMock {
        /**
         * Returns embedding results for the given input text.
         */
        TextEmbeddingResults<?> embeddingResults(String input);

        /**
         * Returns embedding values as a float array for the given input text.
         */
        float[] embedding(String input);

        /**
         * Returns embedding values as a list of floats for the given input text.
         * Default implementation converts the float array to a list.
         */
        default List<Float> embeddingList(String input) {
            float[] embedding = embedding(input);
            List<Float> embeddingList = new ArrayList<>(embedding.length);
            for (float value : embedding) {
                embeddingList.add(value);
            }
            return embeddingList;
        }
    }

    /**
     * Map of embedding model implementations by type.
     */
    private static final Map<TestEmbeddingModel, TextEmbeddingModelMock> TEST_EMBEDDING_MODELS = Map.ofEntries(
        // Float embedding model implementation
        Map.entry(TestEmbeddingModel.FLOAT_EMBEDDING_MODEL, new TextEmbeddingModelMock() {
            @Override
            public TextEmbeddingResults<?> embeddingResults(String input) {
                TextEmbeddingFloatResults.Embedding embedding = new TextEmbeddingFloatResults.Embedding(embedding(input));
                return new TextEmbeddingFloatResults(List.of(embedding));
            }

            @Override
            public float[] embedding(String input) {
                String[] tokens = input.split("\\s+");
                float[] embedding = new float[tokens.length];
                for (int i = 0; i < tokens.length; i++) {
                    embedding[i] = tokens[i].length();
                }
                return embedding;
            }
        }),

        // Byte embedding model implementation
        Map.entry(TestEmbeddingModel.BYTES_EMBEDDING_MODEL, new TextEmbeddingModelMock() {
            @Override
            public TextEmbeddingResults<?> embeddingResults(String input) {
                TextEmbeddingByteResults.Embedding embedding = new TextEmbeddingByteResults.Embedding(bytes(input));
                return new TextEmbeddingBitResults(List.of(embedding));
            }

            private byte[] bytes(String input) {
                return input.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public float[] embedding(String input) {
                return new TextEmbeddingByteResults.Embedding(bytes(input)).toFloatArray();
            }
        }),

        // Bit embedding model implementation
        Map.entry(TestEmbeddingModel.BITS_EMBEDDING_MODEL, new TextEmbeddingModelMock() {
            @Override
            public TextEmbeddingResults<?> embeddingResults(String input) {
                TextEmbeddingByteResults.Embedding embedding = new TextEmbeddingByteResults.Embedding(bytes(input));
                return new TextEmbeddingBitResults(List.of(embedding));
            }

            private byte[] bytes(String input) {
                String[] tokens = input.split("\\s+");
                byte[] embedding = new byte[tokens.length];
                for (int i = 0; i < tokens.length; i++) {
                    embedding[i] = (byte) (tokens[i].length() % 2);
                }
                return embedding;
            }

            @Override
            public float[] embedding(String input) {
                return new TextEmbeddingByteResults.Embedding(bytes(input)).toFloatArray();
            }
        })
    );

    //
    // Thread pool management for async testing
    //
    private ThreadPool threadPool;

    @Before
    public void setThreadPool() {
        threadPool = createThreadPool(
            new FixedExecutorBuilder(
                Settings.EMPTY,
                EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME,
                between(1, 10),
                1024,
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    //
    // Pre-optimizer and inference runner setup
    //

    /**
     * Creates a LogicalPlanPreOptimizer with the specified embedding model.
     *
     * @param textEmbeddingModel the embedding model to use
     * @return a pre-optimizer configured with the specified model
     */
    protected LogicalPlanPreOptimizer preOptimizer(TestEmbeddingModel textEmbeddingModel) {
        LogicalPreOptimizerContext preOptimizerContext = new LogicalPreOptimizerContext(FoldContext.small());
        return new LogicalPlanPreOptimizer(mockTransportActionServices(textEmbeddingModel), preOptimizerContext);
    }

    /**
     * Creates a mock inference runner that uses the specified embedding model.
     *
     * @param textEmbeddingModel the embedding model to use
     * @return a mock inference runner
     */
    protected BulkInferenceRunner mockBulkInferenceRunner(TestEmbeddingModel textEmbeddingModel) {
        Client mockClient = new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action instanceof InferenceAction && request instanceof InferenceAction.Request inferenceRequest) {
                    TextEmbeddingResults<?> inferenceResult = TEST_EMBEDDING_MODELS.get(textEmbeddingModel)
                        .embeddingResults(inferenceRequest.getInput().getFirst());
                    listener.onResponse((Response) new InferenceAction.Response(inferenceResult));
                    return;

                }

                listener.onFailure(new UnsupportedOperationException("Unexpected action: " + action));
            }
        };
        return new BulkInferenceRunner(mockClient, between(1, 10));
    }

    /**
     * Creates mock transport action services with the specified embedding model.
     *
     * @param textEmbeddingModel the embedding model to use
     * @return mock transport action services
     */
    private TransportActionServices mockTransportActionServices(TestEmbeddingModel textEmbeddingModel) {

        InferenceService inferenceService = mock(InferenceService.class);
        when(inferenceService.bulkInferenceRunner()).thenReturn(mockBulkInferenceRunner(textEmbeddingModel));
        return new TransportActionServices(null, null, null, null, null, null, null, inferenceService);
    }

    /**
     * Gets the embedding list for the given model and input text.
     *
     * @param textEmbeddingModel the embedding model to use
     * @param input              the input text
     * @return the embedding as a list of floats
     */
    protected static List<Float> embedding(TestEmbeddingModel textEmbeddingModel, String input) {
        return TEST_EMBEDDING_MODELS.get(textEmbeddingModel).embeddingList(input);
    }

    //
    // Plan and expression generation helpers
    //

    /**
     * Creates a random logical plan for testing.
     * The plan consists of a relation with random commands applied to it.
     *
     * @return a random logical plan
     */
    protected LogicalPlan randomPlan() {
        LogicalPlan plan = EsqlTestUtils.relation();
        int numCommands = between(0, 100);

        for (int i = 0; i < numCommands; i++) {
            plan = switch (randomInt(3)) {
                case 0 -> new Eval(Source.EMPTY, plan, List.of(new Alias(Source.EMPTY, randomIdentifier(), randomExpression())));
                case 1 -> new Limit(Source.EMPTY, of(randomInt()), plan);
                case 2 -> new Filter(Source.EMPTY, plan, randomCondition());
                default -> new Project(Source.EMPTY, plan, List.of(new Alias(Source.EMPTY, randomIdentifier(), fieldAttribute())));
            };
        }
        return plan;
    }

    /**
     * Creates a random expression for testing.
     *
     * @return a random expression
     */
    protected Expression randomExpression() {
        return switch (randomInt(4)) {
            case 0 -> of(randomInt());
            case 1 -> of(randomIdentifier());
            case 2 -> new Add(Source.EMPTY, of(randomInt()), of(randomDouble()));
            case 3 -> new TextEmbedding(Source.EMPTY, of(randomIdentifier()), of(randomIdentifier()));
            default -> new Concat(Source.EMPTY, of(randomIdentifier()), randomList(1, 10, () -> of(randomIdentifier())));
        };
    }

    /**
     * Creates a random condition expression for testing.
     *
     * @return a random condition expression
     */
    protected Expression randomCondition() {
        if (randomBoolean()) {
            return EsqlTestUtils.equalsOf(randomExpression(), randomExpression());
        }

        return EsqlTestUtils.greaterThanOf(randomExpression(), randomExpression());
    }
}
