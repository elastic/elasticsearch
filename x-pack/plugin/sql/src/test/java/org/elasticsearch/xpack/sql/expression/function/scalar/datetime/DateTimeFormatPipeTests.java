/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

public class DateTimeFormatPipeTests extends AbstractNodeTestCase<DateTimeFormatPipe, Pipe> {

    public static DateTimeFormatPipe randomDateTimeFormatPipe() {
        return (DateTimeFormatPipe) new DateTimeFormat(randomSource(), randomDatetimeLiteral(), randomStringLiteral(), randomZone())
            .makePipe();
    }

    @Override
    protected DateTimeFormatPipe randomInstance() {
        return randomDateTimeFormatPipe();
    }

    private Expression randomDateTimeFormatPipeExpression() {
        return randomDateTimeFormatPipe().expression();
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (the two parameters of the binary function) which are tested separately
        DateTimeFormatPipe b1 = randomInstance();

        Expression newExpression = randomValueOtherThan(b1.expression(), this::randomDateTimeFormatPipeExpression);
        DateTimeFormatPipe newB = new DateTimeFormatPipe(b1.source(), newExpression, b1.left(), b1.right(), b1.zoneId());
        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));

        DateTimeFormatPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), SourceTests::randomSource);
        newB = new DateTimeFormatPipe(newLoc, b2.expression(), b2.left(), b2.right(), b2.zoneId());
        assertEquals(newB, b2.transformPropertiesOnly(v -> Objects.equals(v, b2.source()) ? newLoc : v, Source.class));
    }

    @Override
    public void testReplaceChildren() {
        DateTimeFormatPipe b = randomInstance();
        Pipe newLeft = pipe(((Expression) randomValueOtherThan(b.left(), FunctionTestUtils::randomDatetimeLiteral)));
        Pipe newRight = pipe(((Expression) randomValueOtherThan(b.right(), FunctionTestUtils::randomStringLiteral)));
        ZoneId newZoneId = randomValueOtherThan(b.zoneId(), ESTestCase::randomZone);
        DateTimeFormatPipe newB = new DateTimeFormatPipe(b.source(), b.expression(), b.left(), b.right(), newZoneId);
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
    protected DateTimeFormatPipe mutate(DateTimeFormatPipe instance) {
        List<Function<DateTimeFormatPipe, DateTimeFormatPipe>> randoms = new ArrayList<>();
        randoms.add(
            f -> new DateTimeFormatPipe(
                f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), FunctionTestUtils::randomDatetimeLiteral))),
                f.right(),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone)
            )
        );
        randoms.add(
            f -> new DateTimeFormatPipe(
                f.source(),
                f.expression(),
                f.left(),
                pipe(((Expression) randomValueOtherThan(f.right(), FunctionTestUtils::randomStringLiteral))),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone)
            )
        );
        randoms.add(
            f -> new DateTimeFormatPipe(
                f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), FunctionTestUtils::randomDatetimeLiteral))),
                pipe(((Expression) randomValueOtherThan(f.right(), FunctionTestUtils::randomStringLiteral))),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone)
            )
        );

        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected DateTimeFormatPipe copy(DateTimeFormatPipe instance) {
        return new DateTimeFormatPipe(instance.source(), instance.expression(), instance.left(), instance.right(), instance.zoneId());
    }
}
