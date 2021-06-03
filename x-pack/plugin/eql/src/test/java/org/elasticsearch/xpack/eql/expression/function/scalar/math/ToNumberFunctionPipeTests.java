/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.Expressions.pipe;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomIntLiteral;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;

public class ToNumberFunctionPipeTests extends AbstractNodeTestCase<ToNumberFunctionPipe, Pipe> {

    @Override
    protected ToNumberFunctionPipe randomInstance() {
        return randomToNumberFunctionPipe();
    }

    private Expression randomToNumberFunctionExpression() {
        return randomToNumberFunctionPipe().expression();
    }

    public static ToNumberFunctionPipe randomToNumberFunctionPipe() {
        return (ToNumberFunctionPipe) (new ToNumber(
                randomSource(),
                randomStringLiteral(),
                randomFrom(true, false) ? randomIntLiteral() : null)
            .makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (string and base) which are tested separately
        ToNumberFunctionPipe b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomToNumberFunctionExpression());
        ToNumberFunctionPipe newB = new ToNumberFunctionPipe(
                b1.source(),
                newExpression,
                b1.value(),
                b1.base());
        assertEquals(newB, b1.transformPropertiesOnly(Expression.class, v -> Objects.equals(v, b1.expression()) ? newExpression : v));

        ToNumberFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new ToNumberFunctionPipe(
                newLoc,
                b2.expression(),
                b2.value(),
                b2.base());
        assertEquals(newB,
                b2.transformPropertiesOnly(Source.class, v -> Objects.equals(v, b2.source()) ? newLoc : v));
    }

    @Override
    public void testReplaceChildren() {
        ToNumberFunctionPipe b = randomInstance();
        Pipe newValue = randomValueOtherThan(b.value(), () -> pipe(randomStringLiteral()));
        Pipe newBase = b.base() == null ? null : randomValueOtherThan(b.base(), () -> pipe(randomIntLiteral()));
        ToNumberFunctionPipe newB = new ToNumberFunctionPipe(b.source(), b.expression(), b.value(), b.base());

        ToNumberFunctionPipe transformed = newB.replaceChildren(newValue, b.base());
        assertEquals(transformed.value(), newValue);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.base(), b.base());

        transformed = newB.replaceChildren(b.value(), newBase);
        assertEquals(transformed.value(), b.value());
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.base(), newBase);

        transformed = newB.replaceChildren(newValue, newBase);
        assertEquals(transformed.value(), newValue);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.base(), newBase);
    }

    @Override
    protected ToNumberFunctionPipe mutate(ToNumberFunctionPipe instance) {
        List<Function<ToNumberFunctionPipe, ToNumberFunctionPipe>> randoms = new ArrayList<>();
        randoms.add(f -> new ToNumberFunctionPipe(f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.value(), () -> randomStringLiteral()))),
                f.base()));
        randoms.add(f -> new ToNumberFunctionPipe(f.source(),
                f.expression(),
                f.value(),
                f.base() == null ? null : randomValueOtherThan(f.base(), () -> pipe(randomIntLiteral()))));
        randoms.add(f -> new ToNumberFunctionPipe(f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.value(), () -> randomStringLiteral()))),
                f.base() == null ? null : randomValueOtherThan(f.base(), () -> pipe(randomIntLiteral()))));

        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected ToNumberFunctionPipe copy(ToNumberFunctionPipe instance) {
        return new ToNumberFunctionPipe(instance.source(),
                instance.expression(),
                instance.value(),
                instance.base());
    }
}
