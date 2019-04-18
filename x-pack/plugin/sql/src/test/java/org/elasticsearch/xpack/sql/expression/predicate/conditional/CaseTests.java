/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.tree.NodeSubclassTests;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.Arrays;

import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomIntLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;

/**
 * Needed to override tests in {@link NodeSubclassTests} as Case is special since its children are not usual
 * expressions but {@link IfConditional}s.
 */
public class CaseTests extends AbstractNodeTestCase<Case, Expression> {

    public static Case randomCase() {
        return new Case(Source.EMPTY, Arrays.asList(
            new IfConditional(Source.EMPTY, new Equals(Source.EMPTY, randomStringLiteral(), randomStringLiteral()),
                randomIntLiteral()), randomIntLiteral()));
    }

    @Override
    protected Case randomInstance() {
        return randomCase();
    }

    @Override
    protected Case mutate(Case instance) {
        return randomCase();
    }

    @Override
    protected Case copy(Case instance) {
        return new Case(instance.source(), instance.children());
    }

    @Override
    public void testTransform() {
    }

    @Override
    public void testReplaceChildren() {
    }
}
