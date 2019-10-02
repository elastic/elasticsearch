/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.SourceTests;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.expression.Expressions.pipe;
import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.tree.SourceTests.randomSource;

public class BinaryDateTimePipeTests extends AbstractNodeTestCase<BinaryDateTimePipe, Pipe> {

    @Override
    protected BinaryDateTimePipe randomInstance() {
        return randomDateTruncPipe();
    }

    private Expression randomDateTruncPipeExpression() {
        return randomDateTruncPipe().expression();
    }

    public static BinaryDateTimePipe randomDateTruncPipe() {
        return (BinaryDateTimePipe) new DateTrunc(
                randomSource(),
                randomStringLiteral(),
                randomStringLiteral(),
                randomZone())
                .makePipe();
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (the two parameters of the binary function) which are tested separately
        BinaryDateTimePipe b1 = randomInstance();

        Expression newExpression = randomValueOtherThan(b1.expression(), this::randomDateTruncPipeExpression);
        BinaryDateTimePipe newB = new BinaryDateTimePipe(
                b1.source(),
                newExpression,
                b1.left(),
                b1.right(),
                b1.zoneId(),
                b1.operation());
        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));

        BinaryDateTimePipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), SourceTests::randomSource);
        newB = new BinaryDateTimePipe(
                newLoc,
                b2.expression(),
                b2.left(),
                b2.right(),
                b2.zoneId(),
                b2.operation());
        assertEquals(newB,
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.source()) ? newLoc : v, Source.class));
    }

    @Override
    public void testReplaceChildren() {
        BinaryDateTimePipe b = randomInstance();
        Pipe newLeft = pipe(((Expression) randomValueOtherThan(b.left(), FunctionTestUtils::randomStringLiteral)));
        Pipe newRight = pipe(((Expression) randomValueOtherThan(b.right(), FunctionTestUtils::randomDatetimeLiteral)));
        ZoneId newZoneId = randomValueOtherThan(b.zoneId(), ESTestCase::randomZone);
        BinaryDateTimePipe newB = new BinaryDateTimePipe(
            b.source(), b.expression(), b.left(), b.right(), newZoneId, randomFrom(BinaryDateTimeProcessor.BinaryDateOperation.values()));
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
    protected BinaryDateTimePipe mutate(BinaryDateTimePipe instance) {
        List<Function<BinaryDateTimePipe, BinaryDateTimePipe>> randoms = new ArrayList<>();
        randoms.add(f -> new BinaryDateTimePipe(f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), FunctionTestUtils::randomStringLiteral))),
                f.right(),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone),
                randomFrom(BinaryDateTimeProcessor.BinaryDateOperation.values())));
        randoms.add(f -> new BinaryDateTimePipe(f.source(),
                f.expression(),
                f.left(),
                pipe(((Expression) randomValueOtherThan(f.right(), FunctionTestUtils::randomDatetimeLiteral))),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone),
                randomFrom(BinaryDateTimeProcessor.BinaryDateOperation.values())));
        randoms.add(f -> new BinaryDateTimePipe(f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), FunctionTestUtils::randomStringLiteral))),
                pipe(((Expression) randomValueOtherThan(f.right(), FunctionTestUtils::randomDatetimeLiteral))),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone),
                randomFrom(BinaryDateTimeProcessor.BinaryDateOperation.values())));

        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected BinaryDateTimePipe copy(BinaryDateTimePipe instance) {
        return new BinaryDateTimePipe(instance.source(),
                instance.expression(),
                instance.left(),
                instance.right(),
                instance.zoneId(),
                instance.operation());
    }
}
