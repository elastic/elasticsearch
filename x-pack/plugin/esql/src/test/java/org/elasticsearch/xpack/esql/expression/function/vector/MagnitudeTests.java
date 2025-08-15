/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.hamcrest.Matchers.equalTo;

@FunctionName("v_magnitude")
public class MagnitudeTests extends AbstractVectorTestCase {

    public MagnitudeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return scalarParameters(Magnitude.class.getSimpleName(), Magnitude.SCALAR_FUNCTION);
    }

    protected EsqlCapabilities.Cap capability() {
        return EsqlCapabilities.Cap.MAGNITUDE_SCALAR_VECTOR_FUNCTION;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Magnitude(source, args.get(0));
    }

    @Before
    public void checkCapability() {
        assumeTrue("Scalar function is not enabled", capability().isEnabled());
    }

    protected static Iterable<Object[]> scalarParameters(String className, Magnitude.ScalarEvaluatorFunction scalarFunction) {

        final String evaluatorName = className + "Evaluator" + "[child=Attribute[channel=0]]";

        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Basic test with a dense vector.
        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR), () -> {
            int dimensions = between(64, 128);
            List<Float> input = randomDenseVector(dimensions);
            float[] array = listToFloatArray(input);
            double expected = scalarFunction.calculateScalar(array);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(input, DENSE_VECTOR, "vector")),
                evaluatorName,
                DOUBLE,
                equalTo(expected)
            );
        }));

        return parameterSuppliersFromTypedData(suppliers);
    }
}
