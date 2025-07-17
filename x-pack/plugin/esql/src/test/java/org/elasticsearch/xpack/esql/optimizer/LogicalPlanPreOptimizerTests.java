/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.inference.InferenceServices;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogicalPlanPreOptimizerTests extends ESTestCase {

    public void testPlanIsMarkedAsPreOptimized() throws Exception {
        for (int round = 0; round < 100; round++) {
            // We want to make sure that the pre-optimizer woks for a wide range of plans
            preOptimizedPlan(randomPlan());
        }
    }

    public void testPreOptimizeFailsIfPlanIsNotAnalyzed() throws Exception {
        LogicalPlan plan = EsqlTestUtils.relation();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();

        preOptimizer().preOptimize(plan, ActionListener.wrap(r -> fail("Should have failed"), exceptionHolder::set));
        assertBusy(() -> {
            assertThat(exceptionHolder.get(), notNullValue());
            IllegalStateException e = as(exceptionHolder.get(), IllegalStateException.class);
            assertThat(e.getMessage(), equalTo("Expected analyzed plan"));
        });
    }

    public void testEvalFunctionEmbeddingBytes() throws Exception {
        testEvalFunctionEmbedding(BYTES_EMBEDDING_MODEL);
    }

    public void testEvalFunctionEmbeddingBits() throws Exception {
        testEvalFunctionEmbedding(BIT_EMBEDDING_MODEL);
    }

    public void testEvalFunctionEmbeddingFloats() throws Exception {
        testEvalFunctionEmbedding(FLOAT_EMBEDDING_MODEL);
    }

    public void testKnnFunctionEmbeddingBytes() throws Exception {
        testKnnFunctionEmbedding(BYTES_EMBEDDING_MODEL);
    }

    public void testKnnFunctionEmbeddingBits() throws Exception {
        testKnnFunctionEmbedding(BIT_EMBEDDING_MODEL);
    }

    public void testKnnFunctionEmbeddingFloats() throws Exception {
        testKnnFunctionEmbedding(FLOAT_EMBEDDING_MODEL);
    }

    public LogicalPlan preOptimizedPlan(LogicalPlan plan) throws Exception {
        return preOptimizedPlan(preOptimizer(), plan);
    }

    public LogicalPlan preOptimizedPlan(LogicalPlanPreOptimizer preOptimizer, LogicalPlan plan) throws Exception {
        // set plan as analyzed
        plan.setPreOptimized();

        SetOnce<LogicalPlan> resultHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();

        preOptimizer.preOptimize(plan, ActionListener.wrap(resultHolder::set, exceptionHolder::set));

        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        assertThat(resultHolder.get(), notNullValue());
        assertThat(resultHolder.get().preOptimized(), equalTo(true));

        return resultHolder.get();
    }

    private void testEvalFunctionEmbedding(TextEmbeddingModelMock textEmbeddingModel) throws Exception {
        String inferenceId = randomUUID();
        String query = String.join(" ", randomArray(1, String[]::new, () -> randomAlphaOfLength(randomIntBetween(1, 10))));
        String fieldName = randomIdentifier();

        LogicalPlanPreOptimizer preOptimizer = preOptimizer(textEmbeddingModel);
        EsRelation relation = relation();
        Eval eval = new Eval(
            Source.EMPTY,
            relation,
            List.of(new Alias(Source.EMPTY, fieldName, new TextEmbedding(Source.EMPTY, of(query), of(inferenceId))))
        );
        eval.setAnalyzed();

        Eval preOptimizedEval = as(preOptimizedPlan(preOptimizer, eval), Eval.class);
        assertThat(preOptimizedEval.fields(), hasSize(1));
        assertThat(preOptimizedEval.fields().get(0).name(), equalTo(fieldName));
        Literal preOptimizedQuery = as(preOptimizedEval.fields().get(0).child(), Literal.class);
        assertThat(preOptimizedQuery.dataType(), equalTo(DENSE_VECTOR));
        assertThat(preOptimizedQuery.value(), equalTo(textEmbeddingModel.embeddingList(query)));

    }

    private void testKnnFunctionEmbedding(TextEmbeddingModelMock textEmbeddingModel) throws Exception {
        String inferenceId = randomUUID();
        String query = String.join(" ", randomArray(1, String[]::new, () -> randomAlphaOfLength(randomIntBetween(1, 10))));

        LogicalPlanPreOptimizer preOptimizer = preOptimizer(textEmbeddingModel);
        EsRelation relation = relation();
        Filter filter = new Filter(
            Source.EMPTY,
            relation,
            new Knn(Source.EMPTY, getFieldAttribute("a"), new TextEmbedding(Source.EMPTY, of(query), of(inferenceId)), of(10), null)
        );
        Knn knn = as(filter.condition(), Knn.class);

        Filter preOptimizedFilter = as(preOptimizedPlan(preOptimizer, filter), Filter.class);
        Knn preOptimizedKnn = as(preOptimizedFilter.condition(), Knn.class);
        assertThat(preOptimizedKnn.field(), equalTo(knn.field()));
        assertThat(preOptimizedKnn.k(), equalTo(knn.k()));
        assertThat(preOptimizedKnn.options(), equalTo(knn.options()));

        Literal preOptimizedQuery = as(preOptimizedKnn.query(), Literal.class);
        assertThat(preOptimizedQuery.dataType(), equalTo(DENSE_VECTOR));
        assertThat(preOptimizedQuery.value(), equalTo(textEmbeddingModel.embeddingList(query)));
    }

    private static LogicalPlanPreOptimizer preOptimizer() {
        return preOptimizer(randomFrom(FLOAT_EMBEDDING_MODEL, BYTES_EMBEDDING_MODEL, BIT_EMBEDDING_MODEL));
    }

    private static LogicalPlanPreOptimizer preOptimizer(TextEmbeddingModelMock textEmbeddingModel) {
        return preOptimizer(mockInferenceRunner(textEmbeddingModel));
    }

    public static LogicalPlanPreOptimizer preOptimizer(InferenceRunner inferenceRunner) {
        LogicalPreOptimizerContext preOptimizerContext = new LogicalPreOptimizerContext(FoldContext.small());
        return new LogicalPlanPreOptimizer(mockTransportActionServices(inferenceRunner), preOptimizerContext);
    }

    private LogicalPlan randomPlan() {
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

    private Expression randomExpression() {
        return switch (randomInt(4)) {
            case 0 -> of(randomInt());
            case 1 -> of(randomIdentifier());
            case 2 -> new Add(Source.EMPTY, of(randomInt()), of(randomDouble()));
            case 3 -> new TextEmbedding(Source.EMPTY, of(randomIdentifier()), of(randomIdentifier()));
            default -> new Concat(Source.EMPTY, of(randomIdentifier()), randomList(1, 10, () -> of(randomIdentifier())));
        };
    }

    private Expression randomCondition() {
        if (randomBoolean()) {
            return EsqlTestUtils.equalsOf(randomExpression(), randomExpression());
        }

        return EsqlTestUtils.greaterThanOf(randomExpression(), randomExpression());
    }

    private static TransportActionServices mockTransportActionServices(InferenceRunner inferenceRunner) {
        InferenceServices inferenceServices = mock(InferenceServices.class);
        when(inferenceServices.inferenceRunner()).thenReturn(inferenceRunner);
        return new TransportActionServices(null, null, null, null, null, null, null, inferenceServices);
    }

    private static InferenceRunner mockInferenceRunner(TextEmbeddingModelMock textEmbeddingModel) {
        return new InferenceRunner() {
            @Override
            public void execute(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
                listener.onResponse(new InferenceAction.Response(textEmbeddingModel.embeddingResults(request.getInput().getFirst())));
            }

            @Override
            public void executeBulk(BulkInferenceRequestIterator requests, ActionListener<List<InferenceAction.Response>> listener) {
                listener.onFailure(
                    new UnsupportedOperationException("executeBulk should not be invoked for plans without inference functions")
                );
            }
        };
    }

    private interface TextEmbeddingModelMock {
        TextEmbeddingResults<?> embeddingResults(String input);

        float[] embedding(String input);

        default List<Float> embeddingList(String input) {
            float[] embedding = embedding(input);
            List<Float> embeddingList = new ArrayList<>(embedding.length);
            for (float value : embedding) {
                embeddingList.add(value);
            }
            return embeddingList;
        }
    }

    private static final TextEmbeddingModelMock FLOAT_EMBEDDING_MODEL = new TextEmbeddingModelMock() {
        public TextEmbeddingResults<?> embeddingResults(String input) {
            TextEmbeddingFloatResults.Embedding embedding = new TextEmbeddingFloatResults.Embedding(embedding(input));
            return new TextEmbeddingFloatResults(List.of(embedding));
        }

        public float[] embedding(String input) {
            String[] tokens = input.split("\\s+");
            float[] embedding = new float[tokens.length];
            for (int i = 0; i < tokens.length; i++) {
                embedding[i] = tokens[i].length();
            }
            return embedding;
        }
    };

    private static final TextEmbeddingModelMock BYTES_EMBEDDING_MODEL = new TextEmbeddingModelMock() {
        public TextEmbeddingResults<?> embeddingResults(String input) {
            TextEmbeddingByteResults.Embedding embedding = new TextEmbeddingByteResults.Embedding(bytes(input));
            return new TextEmbeddingBitResults(List.of(embedding));
        }

        private byte[] bytes(String input) {
            return input.getBytes(StandardCharsets.UTF_8);
        }

        public float[] embedding(String input) {
            return new TextEmbeddingByteResults.Embedding(bytes(input)).toFloatArray();
        }
    };

    private static final TextEmbeddingModelMock BIT_EMBEDDING_MODEL = new TextEmbeddingModelMock() {
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

        public float[] embedding(String input) {
            return new TextEmbeddingByteResults.Embedding(bytes(input)).toFloatArray();
        }
    };
}
