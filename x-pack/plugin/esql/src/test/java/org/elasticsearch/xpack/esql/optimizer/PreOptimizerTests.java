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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class PreOptimizerTests extends ESTestCase {

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

    private void testEvalFunctionEmbedding(TextEmbeddingModelMock textEmbeddingModel) throws Exception {
        String inferenceId = randomUUID();
        String query = String.join(" ", randomArray(1, String[]::new, () -> randomAlphaOfLength(randomIntBetween(1, 10))));
        String fieldName = randomIdentifier();

        PreOptimizer preOptimizer = new PreOptimizer(mockInferenceRunner(textEmbeddingModel), FoldContext.small());
        EsRelation relation = relation();
        Eval eval = new Eval(
            Source.EMPTY,
            relation,
            List.of(new Alias(Source.EMPTY, fieldName, new TextEmbedding(Source.EMPTY, of(query), of(inferenceId))))
        );

        SetOnce<Object> preOptimizedPlanHolder = new SetOnce<>();
        preOptimizer.preOptimize(eval, ActionListener.wrap(preOptimizedPlanHolder::set, ESTestCase::fail));

        assertBusy(() -> {
            assertThat(preOptimizedPlanHolder.get(), notNullValue());
            Eval preOptimizedEval = as(preOptimizedPlanHolder.get(), Eval.class);
            assertThat(preOptimizedEval.fields(), hasSize(1));
            assertThat(preOptimizedEval.fields().get(0).name(), equalTo(fieldName));
            Literal preOptimizedQuery = as(preOptimizedEval.fields().get(0).child(), Literal.class);
            assertThat(preOptimizedQuery.dataType(), equalTo(DENSE_VECTOR));
            assertThat(preOptimizedQuery.value(), equalTo(textEmbeddingModel.embedding(query)));
        });
    }

    private void testKnnFunctionEmbedding(TextEmbeddingModelMock textEmbeddingModel) throws Exception {
        String inferenceId = randomUUID();
        String query = String.join(" ", randomArray(1, String[]::new, () -> randomAlphaOfLength(randomIntBetween(1, 10))));

        PreOptimizer preOptimizer = new PreOptimizer(mockInferenceRunner(textEmbeddingModel), FoldContext.small());
        EsRelation relation = relation();
        Filter filter = new Filter(
            Source.EMPTY,
            relation,
            new Knn(Source.EMPTY, getFieldAttribute("a"), new TextEmbedding(Source.EMPTY, of(query), of(inferenceId)), of(10), null)
        );
        Knn knn = as(filter.condition(), Knn.class);

        SetOnce<Object> preOptimizedHolder = new SetOnce<>();
        preOptimizer.preOptimize(filter, ActionListener.wrap(preOptimizedHolder::set, ESTestCase::fail));

        assertBusy(() -> {
            assertThat(preOptimizedHolder.get(), notNullValue());
            Filter preOptimizedFilter = as(preOptimizedHolder.get(), Filter.class);
            Knn preOptimizedKnn = as(preOptimizedFilter.condition(), Knn.class);
            assertThat(preOptimizedKnn.field(), equalTo(knn.field()));
            assertThat(preOptimizedKnn.k(), equalTo(knn.k()));
            assertThat(preOptimizedKnn.options(), equalTo(knn.options()));

            Literal preOptimizedQuery = as(preOptimizedKnn.query(), Literal.class);
            assertThat(preOptimizedQuery.dataType(), equalTo(DENSE_VECTOR));
            assertThat(preOptimizedQuery.value(), equalTo(textEmbeddingModel.embedding(query)));
        });
    }

    private InferenceRunner mockInferenceRunner(TextEmbeddingModelMock textEmbeddingModel) {
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
            return input.getBytes();
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
