/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractVectorSimilarityFunctionTestCase extends AbstractScalarFunctionTestCase {

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

    private static float[] listToFloatArray(List<Float> floatList) {
        float[] floatArray = new float[floatList.size()];
        for (int i = 0; i < floatList.size(); i++) {
            floatArray[i] = floatList.get(i);
        }
        return floatArray;
    }

    protected double calculateSimilarity(List<Float> left, List<Float> right) {
        return 0;
    }

    /**
     * @return A random dense vector for testing
     * @param dimensions
     */
    private static List<Float> randomDenseVector(int dimensions) {
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dimensions; i++) {
            vector.add(randomFloat());
        }
        return vector;
    }

    @Override
    protected Matcher<Object> allNullsMatcher() {
        // A null value on the left or right vector. Similarity is 0
        return equalTo(0.0);
    }
}
