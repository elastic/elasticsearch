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
 * Base class for the tests of {@link InferenceFunction}s taking {@code (input, inference_id [, options])}, where
 * {@code options} is an optional trailing map expression.
 * <p>
 * These extend {@link AbstractFunctionTestCase} directly rather than {@link AbstractScalarFunctionTestCase}:
 * inference functions have no per-row evaluator and are never executed as ordinary scalar functions. They can only
 * be folded, via a dedicated pre-optimizer pass ({@code FoldInferenceFunctions}) that runs a real inference call
 * outside {@link org.elasticsearch.xpack.esql.core.expression.Expression#fold}, so the evaluator-based test
 * machinery in {@link AbstractScalarFunctionTestCase} does not apply here. That folding behavior is already covered
 * end-to-end in {@code InferenceFunctionEvaluatorTests}; subclasses are limited to exercising {@code resolveType}
 * and the declared type signatures.
 */
public abstract class AbstractInferenceFunctionTestCase extends AbstractFunctionTestCase {

    protected AbstractInferenceFunctionTestCase(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * Build the inference function under test.
     *
     * @param options the trailing options map, or {@code null} for test cases exercising the two argument form
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
     * Inference functions require their {@code input}/{@code inference_id} arguments to be foldable, so (unlike
     * {@link AbstractScalarFunctionTestCase}, which checks this via per-row evaluation) there is nothing that
     * otherwise confirms a declared {@link TestCaseSupplier} case actually produces a resolving expression once
     * built. This guards against a case whose declared types look right but whose built expression does not
     * actually resolve, e.g. because {@link #buildFunction} dropped an argument or an options map is malformed.
     */
    public final void testResolvesWithLiteralArguments() {
        Expression expression = buildLiteralExpression(testCase);
        assertTrue(expression.typeResolved().message(), expression.typeResolved().resolved());
    }
}
