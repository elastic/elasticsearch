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
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFormatProcessor.Formatter;

public class DateTimeFormatPipeTests extends AbstractNodeTestCase<DateTimeFormatPipe, Pipe> {

    public static DateTimeFormatPipe randomDateTimeFormatPipe() {
        List<Pipe> functions = new ArrayList<>();
        functions.add(new DateTimeFormat(randomSource(), randomDatetimeLiteral(), randomStringLiteral(), randomZone())
            .makePipe());
        functions.add(new Format(randomSource(), randomDatetimeLiteral(), randomStringLiteral(), randomZone())
            .makePipe());
        return (DateTimeFormatPipe) randomFrom(functions);
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
        DateTimeFormatPipe newB = new DateTimeFormatPipe(b1.source(), newExpression, b1.left(), b1.right(), b1.zoneId(), b1.formatter());
        assertEquals(newB, b1.transformPropertiesOnly(Expression.class, v -> Objects.equals(v, b1.expression()) ? newExpression : v));

        DateTimeFormatPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), SourceTests::randomSource);
        newB = new DateTimeFormatPipe(newLoc, b2.expression(), b2.left(), b2.right(), b2.zoneId(), b2.formatter());
        assertEquals(newB, b2.transformPropertiesOnly(Source.class, v -> Objects.equals(v, b2.source()) ? newLoc : v));

        DateTimeFormatPipe b3 = randomInstance();
        Formatter newFormatter = randomValueOtherThan(b3.formatter(), () -> randomFrom(Formatter.values()));
        newB = new DateTimeFormatPipe(b3.source(), b3.expression(), b3.left(), b3.right(), b3.zoneId(), newFormatter);
        assertEquals(newB, b3.transformPropertiesOnly(Formatter.class, v -> Objects.equals(v, b3.formatter()) ? newFormatter : v));

        DateTimeFormatPipe b4 = randomInstance();
        ZoneId newZI = randomValueOtherThan(b4.zoneId(), ESTestCase::randomZone);
        newB = new DateTimeFormatPipe(b4.source(), b4.expression(), b4.left(), b4.right(), newZI, b4.formatter());
        assertEquals(newB, b4.transformPropertiesOnly(ZoneId.class, v -> Objects.equals(v, b4.zoneId()) ? newZI : v));
    }

    @Override
    public void testReplaceChildren() {
        DateTimeFormatPipe b = randomInstance();
        Pipe newLeft = pipe(((Expression) randomValueOtherThan(b.left(), FunctionTestUtils::randomDatetimeLiteral)));
        Pipe newRight = pipe(((Expression) randomValueOtherThan(b.right(), FunctionTestUtils::randomStringLiteral)));
        ZoneId newZoneId = randomValueOtherThan(b.zoneId(), ESTestCase::randomZone);
        DateTimeFormatPipe newB = new DateTimeFormatPipe(b.source(), b.expression(), b.left(), b.right(), newZoneId, b.formatter());
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
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone),
                f.formatter()
            )
        );
        randoms.add(
            f -> new DateTimeFormatPipe(
                f.source(),
                f.expression(),
                f.left(),
                pipe(((Expression) randomValueOtherThan(f.right(), FunctionTestUtils::randomStringLiteral))),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone),
                f.formatter()
            )
        );
        randoms.add(
            f -> new DateTimeFormatPipe(
                f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), FunctionTestUtils::randomDatetimeLiteral))),
                pipe(((Expression) randomValueOtherThan(f.right(), FunctionTestUtils::randomStringLiteral))),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone),
                f.formatter()
            )
        );
        randoms.add(
            f -> new DateTimeFormatPipe(
                f.source(),
                f.expression(),
                f.left(),
                f.right(),
                f.zoneId(),
                randomValueOtherThan(f.formatter(), () -> randomFrom(Formatter.values()))
            )
        );

        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected DateTimeFormatPipe copy(DateTimeFormatPipe instance) {
        return new DateTimeFormatPipe(instance.source(),
            instance.expression(),
            instance.left(),
            instance.right(),
            instance.zoneId(),
            instance.formatter());
    }
}
