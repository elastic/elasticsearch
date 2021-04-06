/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.Expressions.pipe;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;

public class BinaryStringStringPipeTests
        extends AbstractNodeTestCase<BinaryStringStringPipe, Pipe> {

    @Override
    protected BinaryStringStringPipe randomInstance() {
        return randomBinaryStringStringPipe();
    }

    private Expression randomBinaryStringStringExpression() {
        return randomBinaryStringStringPipe().expression();
    }

    public static BinaryStringStringPipe randomBinaryStringStringPipe() {
        List<Pipe> functions = new ArrayList<>();
        functions.add(new Position(
                randomSource(),
                randomStringLiteral(),
                randomStringLiteral()
                ).makePipe());
        // if we decide to add DIFFERENCE(string,string) in the future, here we'd add it as well
        return (BinaryStringStringPipe) randomFrom(functions);
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (the two parameters of the binary function) which are tested separately
        BinaryStringStringPipe b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomBinaryStringStringExpression());
        BinaryStringStringPipe newB = new BinaryStringStringPipe(
                b1.source(),
                newExpression,
                b1.left(),
                b1.right(),
                b1.operation());
        assertEquals(newB, b1.transformPropertiesOnly(Expression.class, v -> Objects.equals(v, b1.expression()) ? newExpression : v));

        BinaryStringStringPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new BinaryStringStringPipe(
            newLoc,
            b2.expression(),
            b2.left(),
            b2.right(),
            b2.operation());
        assertEquals(newB,
            b2.transformPropertiesOnly(Source.class, v -> Objects.equals(v, b2.source()) ? newLoc : v));
    }

    @Override
    public void testReplaceChildren() {
        BinaryStringStringPipe b = randomInstance();
        Pipe newLeft = pipe(((Expression) randomValueOtherThan(b.left(), () -> randomStringLiteral())));
        Pipe newRight = pipe(((Expression) randomValueOtherThan(b.right(), () -> randomStringLiteral())));
        BinaryStringStringPipe newB =
                new BinaryStringStringPipe(b.source(), b.expression(), b.left(), b.right(), b.operation());

        BinaryPipe transformed = newB.replaceChildren(newLeft, b.right());
        assertEquals(transformed.left(), newLeft);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.right(), b.right());

        transformed = newB.replaceChildren(b.left(), newRight);
        assertEquals(transformed.left(), b.left());
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.right(), newRight);

        transformed = newB.replaceChildren(newLeft, newRight);
        assertEquals(transformed.left(), newLeft);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.right(), newRight);
    }

    @Override
    protected BinaryStringStringPipe mutate(BinaryStringStringPipe instance) {
        List<Function<BinaryStringStringPipe, BinaryStringStringPipe>> randoms = new ArrayList<>();
        randoms.add(f -> new BinaryStringStringPipe(f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), () -> randomStringLiteral()))),
                f.right(),
                f.operation()));
        randoms.add(f -> new BinaryStringStringPipe(f.source(),
                f.expression(),
                f.left(),
                pipe(((Expression) randomValueOtherThan(f.right(), () -> randomStringLiteral()))),
                f.operation()));
        randoms.add(f -> new BinaryStringStringPipe(f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), () -> randomStringLiteral()))),
                pipe(((Expression) randomValueOtherThan(f.right(), () -> randomStringLiteral()))),
                f.operation()));

        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected BinaryStringStringPipe copy(BinaryStringStringPipe instance) {
        return new BinaryStringStringPipe(instance.source(),
                instance.expression(),
                instance.left(),
                instance.right(),
                instance.operation());
    }
}
