/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.testfunction;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.runtime.RuntimeEvaluatorGenerator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit test for runtime-generated ES|QL function evaluators.
 * <p>
 * This test proves that:
 * <ul>
 *   <li>Runtime bytecode generation works correctly</li>
 *   <li>Generated evaluator classes can be instantiated</li>
 *   <li>The Abs3 function definition is correct</li>
 * </ul>
 */
public class RuntimeFunctionTests extends ESTestCase {

    /**
     * Test that the RuntimeEvaluatorGenerator can generate bytecode for Abs3.
     */
    public void testGenerateEvaluatorForAbs3Double() throws Exception {
        // Get the processDouble method from Abs3
        Method processMethod = Abs3.class.getMethod("processDouble", double.class);
        assertThat(processMethod, notNullValue());

        // Verify it has the @RuntimeEvaluator annotation
        assertThat(processMethod.isAnnotationPresent(org.elasticsearch.compute.ann.RuntimeEvaluator.class), equalTo(true));

        // Generate the evaluator class
        RuntimeEvaluatorGenerator generator = RuntimeEvaluatorGenerator.getInstance();
        Class<?> evaluatorClass = generator.getOrGenerateEvaluator(processMethod);

        // Verify the class was generated
        assertThat(evaluatorClass, notNullValue());
        assertThat(evaluatorClass.getSimpleName(), equalTo("Abs3DoubleEvaluator"));

        // Verify it implements ExpressionEvaluator
        assertThat(EvalOperator.ExpressionEvaluator.class.isAssignableFrom(evaluatorClass), equalTo(true));
    }

    /**
     * Test that evaluators can be generated for all Abs3 data types.
     */
    public void testGenerateEvaluatorsForAllTypes() throws Exception {
        RuntimeEvaluatorGenerator generator = RuntimeEvaluatorGenerator.getInstance();

        // Test Double evaluator
        Method doubleMethod = Abs3.class.getMethod("processDouble", double.class);
        Class<?> doubleEvaluator = generator.getOrGenerateEvaluator(doubleMethod);
        assertThat(doubleEvaluator.getSimpleName(), equalTo("Abs3DoubleEvaluator"));

        // Test Long evaluator
        Method longMethod = Abs3.class.getMethod("processLong", long.class);
        Class<?> longEvaluator = generator.getOrGenerateEvaluator(longMethod);
        assertThat(longEvaluator.getSimpleName(), equalTo("Abs3LongEvaluator"));

        // Test Int evaluator
        Method intMethod = Abs3.class.getMethod("processInt", int.class);
        Class<?> intEvaluator = generator.getOrGenerateEvaluator(intMethod);
        assertThat(intEvaluator.getSimpleName(), equalTo("Abs3IntEvaluator"));

        // Test UnsignedLong evaluator
        Method unsignedLongMethod = Abs3.class.getMethod("processUnsignedLong", long.class);
        Class<?> unsignedLongEvaluator = generator.getOrGenerateEvaluator(unsignedLongMethod);
        assertThat(unsignedLongEvaluator.getSimpleName(), equalTo("Abs3UnsignedLongEvaluator"));
    }

    /**
     * Test that the generated evaluator class has the expected structure.
     */
    public void testGeneratedEvaluatorStructure() throws Exception {
        Method processMethod = Abs3.class.getMethod("processDouble", double.class);
        RuntimeEvaluatorGenerator generator = RuntimeEvaluatorGenerator.getInstance();
        Class<?> evaluatorClass = generator.getOrGenerateEvaluator(processMethod);

        // Verify it implements ExpressionEvaluator (which has the eval(Page) method)
        assertThat(EvalOperator.ExpressionEvaluator.class.isAssignableFrom(evaluatorClass), equalTo(true));

        // Verify the class has the expected methods
        Method evalMethod = evaluatorClass.getMethod("eval", org.elasticsearch.compute.data.Page.class);
        assertThat(evalMethod, notNullValue());
        assertThat(evalMethod.getReturnType(), equalTo(org.elasticsearch.compute.data.Block.class));

        // Verify it has a constructor with the expected signature
        var constructor = evaluatorClass.getConstructor(Source.class, EvalOperator.ExpressionEvaluator.class, DriverContext.class);
        assertThat(constructor, notNullValue());
    }

    /**
     * Test that evaluator caching works correctly.
     */
    public void testEvaluatorCaching() throws Exception {
        RuntimeEvaluatorGenerator generator = RuntimeEvaluatorGenerator.getInstance();
        Method processMethod = Abs3.class.getMethod("processDouble", double.class);

        // Generate evaluator first time
        Class<?> evaluator1 = generator.getOrGenerateEvaluator(processMethod);

        // Generate evaluator second time - should return cached instance
        Class<?> evaluator2 = generator.getOrGenerateEvaluator(processMethod);

        // Verify same class instance is returned (cached)
        assertThat(evaluator1, equalTo(evaluator2));
        assertThat(System.identityHashCode(evaluator1), equalTo(System.identityHashCode(evaluator2)));
    }

    /**
     * Test that the TestFunctionPlugin correctly provides Abs3.
     */
    public void testPluginProvidesAbs3() {
        TestFunctionPlugin plugin = new TestFunctionPlugin();
        var functions = plugin.getEsqlFunctions();

        assertThat(functions, notNullValue());
        assertThat(functions.size(), equalTo(1));

        var functionDef = functions.iterator().next();
        assertThat(functionDef.name(), equalTo("abs3"));
    }

    /**
     * Test that Abs3 has the correct NamedWriteable entry.
     */
    public void testAbs3NamedWriteable() {
        assertThat(Abs3.ENTRY, notNullValue());
        assertThat(Abs3.ENTRY.name, equalTo("Abs3"));
        assertThat(Abs3.ENTRY.categoryClass, equalTo(org.elasticsearch.xpack.esql.core.expression.Expression.class));
    }

    /**
     * Test that the generated evaluator can be instantiated and used to evaluate data.
     * This is the end-to-end test that proves the runtime bytecode generation works.
     */
    public void testEvaluatorExecution() throws Exception {
        // Generate the evaluator class
        Method processMethod = Abs3.class.getMethod("processDouble", double.class);
        RuntimeEvaluatorGenerator generator = RuntimeEvaluatorGenerator.getInstance();
        Class<?> evaluatorClass = generator.getOrGenerateEvaluator(processMethod);

        // Create a BlockFactory and DriverContext for testing
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

        // Create a mock input evaluator that returns a constant value
        double inputValue = -42.5;
        EvalOperator.ExpressionEvaluator inputEvaluator = new EvalOperator.ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                // Return a block with the input value for each position
                try (DoubleVector.FixedBuilder builder = blockFactory.newDoubleVectorFixedBuilder(page.getPositionCount())) {
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        builder.appendDouble(i, inputValue);
                    }
                    return builder.build().asBlock();
                }
            }

            @Override
            public void close() {}

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }
        };

        // Instantiate the generated evaluator
        Constructor<?> constructor = evaluatorClass.getConstructor(
            Source.class,
            EvalOperator.ExpressionEvaluator.class,
            DriverContext.class
        );
        EvalOperator.ExpressionEvaluator evaluator = (EvalOperator.ExpressionEvaluator) constructor.newInstance(
            Source.EMPTY,
            inputEvaluator,
            driverContext
        );

        // Create a test page with 3 positions
        Page testPage = new Page(3);

        // Evaluate
        try (Block resultBlock = evaluator.eval(testPage)) {
            assertThat(resultBlock, notNullValue());
            assertThat(resultBlock instanceof DoubleBlock, equalTo(true));

            DoubleBlock doubleResult = (DoubleBlock) resultBlock;
            assertThat(doubleResult.getPositionCount(), equalTo(3));

            // Verify abs(-42.5) = 42.5 for all positions
            for (int i = 0; i < 3; i++) {
                assertThat(doubleResult.getDouble(i), closeTo(42.5, 0.0001));
            }
        }

        // Clean up
        evaluator.close();
    }
}
