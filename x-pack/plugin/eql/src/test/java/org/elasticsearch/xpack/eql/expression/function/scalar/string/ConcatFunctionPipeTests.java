/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.expression.Expressions.pipe;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;

public class ConcatFunctionPipeTests extends AbstractNodeTestCase<ConcatFunctionPipe, Pipe> {

    @Override
    protected ConcatFunctionPipe randomInstance() {
        return randomConcatFunctionPipe();
    }

    private Expression randomConcatFunctionExpression() {
        return randomConcatFunctionPipe().expression();
    }

    public static ConcatFunctionPipe randomConcatFunctionPipe() {
        int size = randomIntBetween(2, 10);
        List<Expression> addresses = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            addresses.add(randomStringLiteral());
        }
        return (ConcatFunctionPipe) (new Concat(randomSource(), addresses).makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (the values) which are tested separately
        ConcatFunctionPipe b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomConcatFunctionExpression());
        ConcatFunctionPipe newB = new ConcatFunctionPipe(b1.source(), newExpression, b1.values());

        assertEquals(newB, b1.transformPropertiesOnly(Expression.class, v -> Objects.equals(v, b1.expression()) ? newExpression : v));

        ConcatFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new ConcatFunctionPipe(newLoc, b2.expression(), b2.values());

        assertEquals(newB, b2.transformPropertiesOnly(Source.class, v -> Objects.equals(v, b2.source()) ? newLoc : v));
    }

    @Override
    public void testReplaceChildren() {
        ConcatFunctionPipe b = randomInstance();
        List<Pipe> newValues = mutateOneValue(b.values());

        ConcatFunctionPipe newB = new ConcatFunctionPipe(b.source(), b.expression(), b.values());
        ConcatFunctionPipe transformed = (ConcatFunctionPipe) newB.replaceChildrenSameSize(newValues);

        assertEquals(transformed.values(), newValues);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
    }

    @Override
    protected ConcatFunctionPipe mutate(ConcatFunctionPipe instance) {
        return new ConcatFunctionPipe(instance.source(), instance.expression(), mutateOneValue(instance.values()));
    }

    @Override
    protected ConcatFunctionPipe copy(ConcatFunctionPipe instance) {
        return new ConcatFunctionPipe(instance.source(), instance.expression(), instance.values());
    }

    private List<Pipe> mutateOneValue(List<Pipe> oldValues) {
        int size = oldValues.size();
        ArrayList<Pipe> newValues = new ArrayList<>(size);

        int index = randomIntBetween(0, size - 1);
        for (int i = 0; i < size; i++) {
            Pipe p = oldValues.get(i);
            newValues.add(i != index ? p : randomValueOtherThan(p, () -> pipe(randomStringLiteral())));
        }
        return newValues;
    }
}
