/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;

/**
 * Base class for tests of {@link InferenceFunction}s that take {@code (input, inference_id [, options])}, where
 * {@code options} is an optional trailing map expression.
 * <p>
 * Subclasses extend {@link AbstractFunctionTestCase} directly, not {@link AbstractScalarFunctionTestCase}.
 * Inference functions have no per-row evaluator, so they never run as ordinary scalar functions. The only way to
 * resolve one to a value is folding: a dedicated pre-optimizer pass ({@code FoldInferenceFunctions}) makes a real
 * inference call outside {@link org.elasticsearch.xpack.esql.core.expression.Expression#fold}. That means the
 * evaluator-based test machinery in {@link AbstractScalarFunctionTestCase} does not apply here.
 * {@code InferenceFunctionEvaluatorTests} already covers that folding behavior end-to-end, so subclasses only need
 * to exercise {@code resolveType} and the declared type signatures.
 */
public abstract class AbstractInferenceFunctionTestCase extends AbstractFunctionTestCase {

    protected AbstractInferenceFunctionTestCase(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * Build the inference function under test. {@code options} is {@code null} for the two-argument form.
     */
    protected abstract Expression buildFunction(Source source, Expression input, Expression inferenceId, Expression options);

    @Override
    protected final Expression build(Source source, List<Expression> args) {
        return buildFunction(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    /**
     * Inference functions are rewritten into a physical operator before execution, so their
     * {@code writeTo}/{@code getWriteableName} deliberately throw.
     */
    @Override
    protected boolean canSerialize() {
        return false;
    }

    /**
     * Inference functions require their {@code input}/{@code inference_id} arguments to be foldable.
     * {@link AbstractScalarFunctionTestCase} checks that a test case actually resolves by evaluating it row by row,
     * but that machinery doesn't apply here (see the class Javadoc above). Without this test, nothing would catch a
     * {@link TestCaseSupplier} case whose declared types look right but whose built expression doesn't actually
     * resolve, for example because {@link #buildFunction} dropped an argument or an options map is malformed.
     */
    public final void testResolvesWithLiteralArguments() {
        Expression expression = buildLiteralExpression(testCase);
        assertTrue(expression.typeResolved().message(), expression.typeResolved().resolved());
    }
}
