/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.tree.SourceTests;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.Expressions.pipe;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomDatetimeLiteral;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;

public class DateTruncPipeTests extends AbstractNodeTestCase<DateTruncPipe, Pipe> {

    @Override
    protected DateTruncPipe randomInstance() {
        return randomDateTruncPipe();
    }

    private Expression randomDateTruncPipeExpression() {
        return randomDateTruncPipe().expression();
    }

    public static DateTruncPipe randomDateTruncPipe() {
        return (DateTruncPipe) new DateTrunc(randomSource(), randomStringLiteral(), randomDatetimeLiteral(), randomZone()).makePipe();
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (the two parameters of the binary function) which are tested separately
        DateTruncPipe b1 = randomInstance();

        Expression newExpression = randomValueOtherThan(b1.expression(), this::randomDateTruncPipeExpression);
        DateTruncPipe newB = new DateTruncPipe(b1.source(), newExpression, b1.left(), b1.right(), b1.zoneId());
        assertEquals(newB, b1.transformPropertiesOnly(Expression.class, v -> Objects.equals(v, b1.expression()) ? newExpression : v));

        DateTruncPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), SourceTests::randomSource);
        newB = new DateTruncPipe(newLoc, b2.expression(), b2.left(), b2.right(), b2.zoneId());
        assertEquals(newB, b2.transformPropertiesOnly(Source.class, v -> Objects.equals(v, b2.source()) ? newLoc : v));
    }

    @Override
    public void testReplaceChildren() {
        DateTruncPipe b = randomInstance();
        Pipe newLeft = pipe(((Expression) randomValueOtherThan(b.left(), FunctionTestUtils::randomStringLiteral)));
        Pipe newRight = pipe(((Expression) randomValueOtherThan(b.right(), FunctionTestUtils::randomDatetimeLiteral)));
        ZoneId newZoneId = randomValueOtherThan(b.zoneId(), ESTestCase::randomZone);
        DateTruncPipe newB = new DateTruncPipe(b.source(), b.expression(), b.left(), b.right(), newZoneId);
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
    protected DateTruncPipe mutate(DateTruncPipe instance) {
        List<Function<DateTruncPipe, DateTruncPipe>> randoms = new ArrayList<>();
        randoms.add(
            f -> new DateTruncPipe(
                f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), FunctionTestUtils::randomStringLiteral))),
                f.right(),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone)
            )
        );
        randoms.add(
            f -> new DateTruncPipe(
                f.source(),
                f.expression(),
                f.left(),
                pipe(((Expression) randomValueOtherThan(f.right(), FunctionTestUtils::randomDatetimeLiteral))),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone)
            )
        );
        randoms.add(
            f -> new DateTruncPipe(
                f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), FunctionTestUtils::randomStringLiteral))),
                pipe(((Expression) randomValueOtherThan(f.right(), FunctionTestUtils::randomDatetimeLiteral))),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone)
            )
        );

        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected DateTruncPipe copy(DateTruncPipe instance) {
        return new DateTruncPipe(instance.source(), instance.expression(), instance.left(), instance.right(), instance.zoneId());
    }
}
