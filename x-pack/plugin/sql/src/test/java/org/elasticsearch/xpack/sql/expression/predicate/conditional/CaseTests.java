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
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomIntLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.tree.SourceTests.randomSource;

/**
 * Needed to override tests in {@link NodeSubclassTests} as Case is special since its children are not usual
 * expressions but {@link IfConditional}s.
 */
public class CaseTests extends AbstractNodeTestCase<Case, Expression> {

    public static Case randomCase() {
        int noConditionals = randomIntBetween(1, 5);
        List<Expression> expressions = new ArrayList<>(noConditionals + 1);
        for (int i = 0; i < noConditionals; i++) {
            expressions.add(new IfConditional(
                randomSource(), new Equals(randomSource(), randomStringLiteral(), randomStringLiteral()), randomIntLiteral()));

        }
        // default else
        expressions.add(randomIntLiteral());
        return new Case(randomSource(), expressions);
    }

    @Override
    protected Case randomInstance() {
        return randomCase();
    }

    @Override
    protected Case mutate(Case instance) {
        Case c = randomCase();
        return new Case(c.source(), mutateChildren(c));
    }

    @Override
    protected Case copy(Case instance) {
        return new Case(instance.source(), instance.children());
    }

    @Override
    public void testTransform() {
        Case c = randomCase();

        Source newSource = randomValueOtherThan(c.source(), SourceTests::randomSource);
        assertEquals(new Case(c.source(), c.children()),
            c.transformPropertiesOnly(p -> Objects.equals(p, c.source()) ? newSource: p, Object.class));

        String newName = randomValueOtherThan(c.name(), () -> randomAlphaOfLength(5));
        assertEquals(new Case(c.source(), c.children()),
            c.transformPropertiesOnly(p -> Objects.equals(p, c.name()) ? newName : p, Object.class));
    }

    @Override
    public void testReplaceChildren() {
        Case c = randomCase();

        List<Expression> newChildren = mutateChildren(c);
        assertEquals(new Case(c.source(), newChildren), c.replaceChildren(newChildren));
    }

    private List<Expression> mutateChildren(Case c) {
        boolean removeConditional = randomBoolean();
        List<Expression> expressions = new ArrayList<>(c.children().size());
        if (removeConditional) {
            expressions.addAll(c.children().subList(0, c.children().size() - 2));
        } else {
            int rndIdx = randomInt(c.conditions().size());
            for (int i = 0; i < c.conditions().size(); i++) {
                if (i == rndIdx) {
                    expressions.add(new IfConditional(randomValueOtherThan(c.conditions().get(i).source(), SourceTests::randomSource),
                        new Equals(randomSource(), randomStringLiteral(), randomStringLiteral()),
                        randomValueOtherThan(c.conditions().get(i).condition(), FunctionTestUtils::randomStringLiteral)));
                } else {
                    expressions.add(c.conditions().get(i));
                }
            }
        }
        expressions.add(c.elseResult());
        return expressions;
    }
}
