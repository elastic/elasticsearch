/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.tree.SourceTests;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.Expressions.pipe;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;

public class InsensitiveBinaryComparisonPipeTests extends AbstractNodeTestCase<InsensitiveBinaryComparisonPipe, Pipe> {

    @Override
    protected InsensitiveBinaryComparisonPipe randomInstance() {
        return randomInsensitiveBinaryComparisonPipe();
    }

    private Expression randomInsensitiveBinaryComparisonExpression() {
        return randomInsensitiveBinaryComparisonPipe().expression();
    }

    public static InsensitiveBinaryComparisonPipe randomInsensitiveBinaryComparisonPipe() {
        return (InsensitiveBinaryComparisonPipe) (new InsensitiveEquals(
            randomSource(),
            randomStringLiteral(),
            randomStringLiteral(),
            TestUtils.UTC
        ).makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (string and substring) which are tested separately
        InsensitiveBinaryComparisonPipe pipe = randomInstance();
        Expression newExpression = randomValueOtherThan(pipe.expression(), this::randomInsensitiveBinaryComparisonExpression);
        InsensitiveBinaryComparisonPipe newPipe = new InsensitiveBinaryComparisonPipe(
            pipe.source(),
            newExpression,
            pipe.left(),
            pipe.right(),
            pipe.asProcessor().function()
        );
        assertEquals(
            newPipe,
            pipe.transformPropertiesOnly(Expression.class, v -> Objects.equals(v, pipe.expression()) ? newExpression : v)
        );

        InsensitiveBinaryComparisonPipe anotherPipe = randomInstance();
        Source newLoc = randomValueOtherThan(anotherPipe.source(), SourceTests::randomSource);
        newPipe = new InsensitiveBinaryComparisonPipe(
            newLoc,
            anotherPipe.expression(),
            anotherPipe.left(),
            anotherPipe.right(),
            anotherPipe.asProcessor().function()
        );
        assertEquals(newPipe, anotherPipe.transformPropertiesOnly(Source.class, v -> Objects.equals(v, anotherPipe.source()) ? newLoc : v));
    }

    @Override
    public void testReplaceChildren() {
        InsensitiveBinaryComparisonPipe pipe = randomInstance();
        Pipe newLeft = pipe(((Expression) randomValueOtherThan(pipe.left(), FunctionTestUtils::randomStringLiteral)));
        Pipe newRight = pipe(((Expression) randomValueOtherThan(pipe.right(), FunctionTestUtils::randomStringLiteral)));
        InsensitiveBinaryComparisonPipe newPipe = new InsensitiveBinaryComparisonPipe(
            pipe.source(),
            pipe.expression(),
            pipe.left(),
            pipe.right(),
            pipe.asProcessor().function()
        );

        InsensitiveBinaryComparisonPipe transformed = newPipe.replaceChildren(newLeft, pipe.right());
        assertEquals(transformed.source(), pipe.source());
        assertEquals(transformed.expression(), pipe.expression());
        assertEquals(transformed.left(), newLeft);
        assertEquals(transformed.right(), pipe.right());

        transformed = newPipe.replaceChildren(pipe.left(), newRight);
        assertEquals(transformed.source(), pipe.source());
        assertEquals(transformed.expression(), pipe.expression());
        assertEquals(transformed.left(), pipe.left());
        assertEquals(transformed.right(), newRight);

        transformed = newPipe.replaceChildren(newLeft, newRight);
        assertEquals(transformed.source(), pipe.source());
        assertEquals(transformed.expression(), pipe.expression());
        assertEquals(transformed.left(), newLeft);
        assertEquals(transformed.right(), newRight);
    }

    @Override
    protected InsensitiveBinaryComparisonPipe mutate(InsensitiveBinaryComparisonPipe instance) {
        List<Function<InsensitiveBinaryComparisonPipe, InsensitiveBinaryComparisonPipe>> randoms = new ArrayList<>();
        randoms.add(
            f -> new InsensitiveBinaryComparisonPipe(
                f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), FunctionTestUtils::randomStringLiteral))),
                f.right(),
                f.asProcessor().function()
            )
        );
        randoms.add(
            f -> new InsensitiveBinaryComparisonPipe(
                f.source(),
                f.expression(),
                f.left(),
                pipe(((Expression) randomValueOtherThan(f.right(), FunctionTestUtils::randomStringLiteral))),
                f.asProcessor().function()
            )
        );
        randoms.add(
            f -> new InsensitiveBinaryComparisonPipe(
                f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), FunctionTestUtils::randomStringLiteral))),
                pipe(((Expression) randomValueOtherThan(f.right(), FunctionTestUtils::randomStringLiteral))),
                f.asProcessor().function()
            )
        );

        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected InsensitiveBinaryComparisonPipe copy(InsensitiveBinaryComparisonPipe instance) {
        return new InsensitiveBinaryComparisonPipe(
            instance.source(),
            instance.expression(),
            instance.left(),
            instance.right(),
            instance.asProcessor().function()
        );
    }
}
