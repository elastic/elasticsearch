/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.compute.data.Page;
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

public abstract class AbstractVectorScalarFunctionTestCase extends AbstractScalarFunctionTestCase {

    protected AbstractVectorScalarFunctionTestCase(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Before
    public void checkCapability() {
        assumeTrue("Scalar function is not enabled", capability().isEnabled());
    }

    /**
     * Get the capability of the vector similarity function to check
     */
    protected abstract EsqlCapabilities.Cap capability();

    protected static Iterable<Object[]> scalarParameters(String className, VectorScalarFunction.ScalarEvaluatorFunction scalarFunction) {

        final String evaluatorName = className + "Evaluator" + "[child=Attribute[channel=0]]";

        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Basic test with a dense vector.
        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR), () -> {
            int dimensions = between(64, 128);
            List<Float> input = randomDenseVector(dimensions);
            float[] array = listToFloatArray(input);
            double expected = scalarFunction.calculateScalar(array);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(array, DENSE_VECTOR, "vector1")),
                evaluatorName,
                DOUBLE,
                equalTo(expected) // Random vectors should have cosine similarity close to 0
            );
        }));

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Page row(List<Object> values) {
        // Convert from List<float[]> to List<ArrayList<Float>>.
        List<Float> boxed = new ArrayList<>();
        var array = (float[]) values.getFirst();
        for (float v : array) {
            boxed.add(v);
        }
        return super.row(List.of(boxed));
    }

    private static float[] listToFloatArray(List<Float> floatList) {
        float[] floatArray = new float[floatList.size()];
        for (int i = 0; i < floatList.size(); i++) {
            floatArray[i] = floatList.get(i);
        }
        return floatArray;
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
