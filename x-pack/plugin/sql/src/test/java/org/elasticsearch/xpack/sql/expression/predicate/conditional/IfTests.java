/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.tree.NodeSubclassTests;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.SourceTests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomIntLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.tree.SourceTests.randomSource;

/**
 * Needed to override tests in {@link NodeSubclassTests} as If is special since its children are not usual
 * expressions but {@link IfConditional}s.
 */
public class IfTests extends AbstractNodeTestCase<If, Expression> {

    public static If randomIf() {
        return new If(randomSource(), new Equals(randomSource(), randomStringLiteral(), randomStringLiteral()),
            randomIntLiteral(), randomIntLiteral());
    }

    @Override
    protected If randomInstance() {
        return randomIf();
    }

    @Override
    protected If mutate(If instance) {
        If iif = randomIf();
        List<Expression> mutatedChildren = mutateChildren(iif);
        return new If(iif.source(), mutatedChildren.get(0), mutatedChildren.get(1), mutatedChildren.get(2));
    }

    @Override
    protected If copy(If instance) {
        return new If(instance.source(), instance.conditions().get(0).condition(), instance.conditions().get(0).result(),
            instance.defaultElse());
    }

    @Override
    public void testTransform() {
        If iif = randomIf();

        Source newSource = randomValueOtherThan(iif.source(), SourceTests::randomSource);
        assertEquals(new If(iif.source(), iif.conditions().get(0).condition(), iif.conditions().get(0).result(), iif.defaultElse()),
            iif.transformPropertiesOnly(p -> Objects.equals(p, iif.source()) ? newSource: p, Object.class));

        String newName = randomValueOtherThan(iif.name(), () -> randomAlphaOfLength(5));
        assertEquals(new If(iif.source(), iif.conditions().get(0).condition(), iif.conditions().get(0).result(), iif.defaultElse()),
            iif.transformPropertiesOnly(p -> Objects.equals(p, iif.name()) ? newName : p, Object.class));
    }

    @Override
    public void testReplaceChildren() {
        If iif = randomIf();

        List<Expression> newChildren = mutateChildren(iif);
        assertEquals(new If(iif.source(), newChildren.get(0), newChildren.get(1), newChildren.get(2)),
            iif.replaceChildren(Arrays.asList(new IfConditional(iif.source(), newChildren.get(0), newChildren.get(1)),
                newChildren.get(2))));
    }

    private List<Expression> mutateChildren(If iif) {
        List<Expression> expressions = new ArrayList<>(3);
        expressions.add(new Equals(randomSource(), randomStringLiteral(), randomStringLiteral()));
        expressions.add(randomValueOtherThan(iif.conditions().get(0).result(), FunctionTestUtils::randomIntLiteral));
        expressions.add(randomValueOtherThan(iif.defaultElse(), FunctionTestUtils::randomIntLiteral));
        return expressions;
    }
}
