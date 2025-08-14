/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractVectorSimilarityFunctionTestCase extends AbstractVectorTestCase {

    protected AbstractVectorSimilarityFunctionTestCase(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Before
    public void checkCapability() {
        assumeTrue("Similarity function is not enabled", capability().isEnabled());
    }

    /**
     * Get the capability of the vector similarity function to check
     */
    protected abstract EsqlCapabilities.Cap capability();

    protected static Iterable<Object[]> similarityParameters(
        String className,
        VectorSimilarityFunction.SimilarityEvaluatorFunction similarityFunction
    ) {

        final String evaluatorName = className + "Evaluator" + "[left=Attribute[channel=0], right=Attribute[channel=1]]";

        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Basic test with two dense vectors
        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR, DENSE_VECTOR), () -> {
            int dimensions = between(64, 128);
            List<Float> left = randomDenseVector(dimensions);
            List<Float> right = randomDenseVector(dimensions);
            float[] leftArray = listToFloatArray(left);
            float[] rightArray = listToFloatArray(right);
            double expected = similarityFunction.calculateSimilarity(leftArray, rightArray);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, DENSE_VECTOR, "vector1"),
                    new TestCaseSupplier.TypedData(right, DENSE_VECTOR, "vector2")
                ),
                evaluatorName,
                DOUBLE,
                equalTo(expected) // Random vectors should have cosine similarity close to 0
            );
        }));

        return parameterSuppliersFromTypedData(suppliers);
    }
}
