/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute.FieldName;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression.PushedBlockLoaderExpression;
import org.elasticsearch.xpack.esql.stats.DisabledSearchStats;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link VectorSimilarityFunction#tryPushToFieldLoading}. ES|QL folds a one-element multivalue into a scalar,
 * so the literal reaching the pushdown can be a {@link Number} instead of a {@link List}.
 */
public class VectorSimilarityPushdownTests extends ESTestCase {

    private static final FieldAttribute VECTOR_FIELD = new FieldAttribute(
        Source.EMPTY,
        "v",
        new EsField("v", DataType.DENSE_VECTOR, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
    );

    private static final SearchStats INDEXED_VECTOR_STATS = new DisabledSearchStats() {
        @Override
        public boolean isIndexed(FieldName field) {
            return true;
        }
    };

    public void testSingleDimensionScalarLiteralIsPushedAsVector() {
        PushedBlockLoaderExpression pushed = push(0.5f);

        assertNotNull("a folded one-element vector must still be pushed", pushed);
        assertSame(VECTOR_FIELD, pushed.field());
        DenseVectorFieldMapper.VectorSimilarityFunctionConfig config =
            (DenseVectorFieldMapper.VectorSimilarityFunctionConfig) pushed.config();
        assertArrayEquals(new float[] { 0.5f }, config.vector(), 0.0f);
    }

    public void testListLiteralIsPushedAsVector() {
        PushedBlockLoaderExpression pushed = push(List.of(0.5f, 0.25f));

        assertNotNull(pushed);
        DenseVectorFieldMapper.VectorSimilarityFunctionConfig config =
            (DenseVectorFieldMapper.VectorSimilarityFunctionConfig) pushed.config();
        assertArrayEquals(new float[] { 0.5f, 0.25f }, config.vector(), 0.0f);
    }

    private static PushedBlockLoaderExpression push(Object literalValue) {
        VectorSimilarityFunction fn = new L2Norm(
            Source.EMPTY,
            VECTOR_FIELD,
            new Literal(Source.EMPTY, literalValue, DataType.DENSE_VECTOR)
        );
        return fn.tryPushToFieldLoading(INDEXED_VECTOR_STATS);
    }
}
