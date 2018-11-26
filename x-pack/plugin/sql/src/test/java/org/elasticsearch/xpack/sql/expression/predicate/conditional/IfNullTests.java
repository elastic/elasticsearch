/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.tree.LocationTests.randomLocation;
import static org.hamcrest.Matchers.contains;

public class IfNullTests extends AbstractNodeTestCase<IfNull, Expression> {

    public static IfNull randomIfNull() {
        return new IfNull(randomLocation(), randomStringLiteral(), randomStringLiteral());
    }

    @Override
    protected IfNull randomInstance() {
        return randomIfNull();
    }

    @Override
    protected IfNull copy(IfNull instance) {
        return instance;
    }

    @Override
    protected IfNull mutate(IfNull instance) {
        return new IfNull(instance.location(), randomStringLiteral(), randomStringLiteral());
    }

    @Override
    public void testReplaceChildren() {
        Location loc = randomLocation();
        Expression expr1 = randomStringLiteral();
        Expression expr2 = randomStringLiteral();

        Expression newExpr1 = randomStringLiteral();
        Expression newExpr2 = randomStringLiteral();

        IfNull ifNull = new IfNull(loc, expr1, expr2);
        Expression newIfNull = ifNull.replaceChildren(Arrays.asList(newExpr1, newExpr2));
        assertEquals(Coalesce.class, newIfNull.getClass());
        assertEquals(loc, newIfNull.location());
        assertThat(newIfNull.children(), contains(newExpr1, newExpr2));

        newIfNull = ifNull.replaceChildren(Collections.singletonList(newExpr1));
        assertEquals(Coalesce.class, newIfNull.getClass());
        assertEquals(loc, newIfNull.location());
        assertThat(newIfNull.children(), contains(newExpr1));
    }

    @Override
    public void testTransform() {}
}
