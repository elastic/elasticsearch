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

public class DateDiffPipeTests extends AbstractNodeTestCase<DateDiffPipe, Pipe> {

    @Override
    protected DateDiffPipe randomInstance() {
        return randomDateDiffPipe();
    }

    private Expression randomDateDiffPipeExpression() {
        return randomDateDiffPipe().expression();
    }

    public static DateDiffPipe randomDateDiffPipe() {
        return (DateDiffPipe) new DateDiff(
                randomSource(),
                randomStringLiteral(),
                randomDatetimeLiteral(),
                randomDatetimeLiteral(),
                randomZone())
                .makePipe();
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (the three parameters of the function) which are tested separately
        DateDiffPipe b1 = randomInstance();

        Expression newExpression = randomValueOtherThan(b1.expression(), this::randomDateDiffPipeExpression);
        DateDiffPipe newB = new DateDiffPipe(
                b1.source(),
                newExpression,
                b1.first(),
                b1.second(),
                b1.third(),
                b1.zoneId());
        assertEquals(newB, b1.transformPropertiesOnly(Expression.class, v -> Objects.equals(v, b1.expression()) ? newExpression : v));

        DateDiffPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), SourceTests::randomSource);
        newB = new DateDiffPipe(
            newLoc,
            b2.expression(),
            b2.first(),
            b2.second(),
            b2.third(),
            b2.zoneId());
        assertEquals(newB,
            b2.transformPropertiesOnly(Source.class, v -> Objects.equals(v, b2.source()) ? newLoc : v));
    }

    @Override
    public void testReplaceChildren() {
        DateDiffPipe b = randomInstance();
        Pipe newFirst = pipe(((Expression) randomValueOtherThan(b.first(), FunctionTestUtils::randomStringLiteral)));
        Pipe newSecond = pipe(((Expression) randomValueOtherThan(b.second(), FunctionTestUtils::randomDatetimeLiteral)));
        Pipe newThird = pipe(((Expression) randomValueOtherThan(b.third(), FunctionTestUtils::randomDatetimeLiteral)));
        ZoneId newZoneId = randomValueOtherThan(b.zoneId(), ESTestCase::randomZone);
        DateDiffPipe newB = new DateDiffPipe(b.source(), b.expression(), b.first(), b.second(), b.third(), newZoneId);

        ThreeArgsDateTimePipe transformed = newB.replaceChildren(newFirst, b.second(), b.third());
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.first(), newFirst);
        assertEquals(transformed.second(), b.second());
        assertEquals(transformed.third(), b.third());

        transformed = newB.replaceChildren(b.first(), newSecond, b.third());
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.first(), b.first());
        assertEquals(transformed.second(), newSecond);
        assertEquals(transformed.third(), b.third());

        transformed = newB.replaceChildren(b.first(), b.second(), newThird);
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.first(), b.first());
        assertEquals(transformed.second(), b.second());
        assertEquals(transformed.third(), newThird);

        transformed = newB.replaceChildren(newFirst, newSecond, newThird);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.first(), newFirst);
        assertEquals(transformed.second(), newSecond);
        assertEquals(transformed.third(), newThird);
    }

    @Override
    protected DateDiffPipe mutate(DateDiffPipe instance) {
        List<Function<DateDiffPipe, DateDiffPipe>> randoms = new ArrayList<>();
        randoms.add(f -> new DateDiffPipe(f.source(), f.expression(),
                pipe(((Expression) randomValueOtherThan(f.first(), FunctionTestUtils::randomStringLiteral))),
                f.second(),
                f.third(),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone)));
        randoms.add(f -> new DateDiffPipe(f.source(), f.expression(),
                f.first(),
                pipe(((Expression) randomValueOtherThan(f.second(), FunctionTestUtils::randomDatetimeLiteral))),
                f.third(),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone)));
        randoms.add(f -> new DateDiffPipe(f.source(), f.expression(),
                f.first(),
                f.second(),
                pipe(((Expression) randomValueOtherThan(f.third(), FunctionTestUtils::randomDatetimeLiteral))),
            randomValueOtherThan(f.zoneId(), ESTestCase::randomZone)));
        randoms.add(f -> new DateDiffPipe(f.source(), f.expression(),
                pipe(((Expression) randomValueOtherThan(f.first(), FunctionTestUtils::randomStringLiteral))),
                pipe(((Expression) randomValueOtherThan(f.second(), FunctionTestUtils::randomDatetimeLiteral))),
                pipe(((Expression) randomValueOtherThan(f.third(), FunctionTestUtils::randomDatetimeLiteral))),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone)));

        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected DateDiffPipe copy(DateDiffPipe instance) {
        return new DateDiffPipe(
            instance.source(),
            instance.expression(),
            instance.first(),
            instance.second(),
            instance.third(),
            instance.zoneId());
    }
}
