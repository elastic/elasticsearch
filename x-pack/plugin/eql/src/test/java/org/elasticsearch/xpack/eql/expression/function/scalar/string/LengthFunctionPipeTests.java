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

import java.util.Objects;

import static org.elasticsearch.xpack.ql.expression.Expressions.pipe;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;

public class LengthFunctionPipeTests extends AbstractNodeTestCase<LengthFunctionPipe, Pipe> {

    @Override
    protected LengthFunctionPipe randomInstance() {
        return randomLengthFunctionPipe();
    }

    private Expression randomLengthFunctionExpression() {
        return randomLengthFunctionPipe().expression();
    }

    public static LengthFunctionPipe randomLengthFunctionPipe() {
        return (LengthFunctionPipe) (new Length(randomSource(), randomStringLiteral()).makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (the input itself) which are tested separately
        LengthFunctionPipe b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomLengthFunctionExpression());
        LengthFunctionPipe newB = new LengthFunctionPipe(b1.source(), newExpression, b1.input());

        assertEquals(newB, b1.transformPropertiesOnly(Expression.class, v -> Objects.equals(v, b1.expression()) ? newExpression : v));

        LengthFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new LengthFunctionPipe(newLoc, b2.expression(), b2.input());

        assertEquals(newB, b2.transformPropertiesOnly(Source.class, v -> Objects.equals(v, b2.source()) ? newLoc : v));
    }

    @Override
    public void testReplaceChildren() {
        LengthFunctionPipe b = randomInstance();
        Pipe newInput = randomValueOtherThan(b.input(), () -> pipe(randomStringLiteral()));

        LengthFunctionPipe newB = new LengthFunctionPipe(b.source(), b.expression(), b.input());
        LengthFunctionPipe transformed = newB.replaceChildren(newInput);

        assertEquals(transformed.input(), newInput);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
    }

    @Override
    protected LengthFunctionPipe mutate(LengthFunctionPipe instance) {
        return new LengthFunctionPipe(
            instance.source(),
            instance.expression(),
            randomValueOtherThan(instance.input(), () -> pipe(randomStringLiteral()))
        );
    }

    @Override
    protected LengthFunctionPipe copy(LengthFunctionPipe instance) {
        return new LengthFunctionPipe(instance.source(), instance.expression(), instance.input());
    }
}
