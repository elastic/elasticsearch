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
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeParseProcessor.Parser;


public class DateTimeParsePipeTests extends AbstractNodeTestCase<DateTimeParsePipe, Pipe> {
    
    public static DateTimeParsePipe randomDateTimeParsePipe() {
        List<Pipe> functions = new ArrayList<>();
        functions.add(new DateTimeParse(            
                randomSource(),
                randomStringLiteral(),
                randomStringLiteral(),
                randomZone()
        ).makePipe());
        functions.add(new TimeParse(
                randomSource(),
                randomStringLiteral(),
                randomStringLiteral(),
                randomZone()
        ).makePipe());
        functions.add(new DateParse(
                randomSource(),
                randomStringLiteral(),
                randomStringLiteral(),
                randomZone()
        ).makePipe());
        return (DateTimeParsePipe) randomFrom(functions);
    }

    @Override
    protected DateTimeParsePipe randomInstance() {
        return randomDateTimeParsePipe();
    }

    private Expression randomDateTimeParsePipeExpression() {
        return randomDateTimeParsePipe().expression();
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (the two parameters of the binary function) which are tested separately
        DateTimeParsePipe b1 = randomInstance();

        Expression newExpression = randomValueOtherThan(b1.expression(), this::randomDateTimeParsePipeExpression);
        DateTimeParsePipe newB = new DateTimeParsePipe(
                b1.source(), 
                newExpression, 
                b1.left(), 
                b1.right(), 
                b1.zoneId(), 
                b1.parser());
        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));

        DateTimeParsePipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), SourceTests::randomSource);
        newB = new DateTimeParsePipe(newLoc, b2.expression(), b2.left(), b2.right(), b2.zoneId(), b2.parser());
        assertEquals(newB, b2.transformPropertiesOnly(v -> Objects.equals(v, b2.source()) ? newLoc : v, Source.class));
    
        DateTimeParsePipe b3 = randomInstance();
        Parser newPr = randomValueOtherThan(b3.parser(), () -> randomFrom(Parser.values()));
        newB = new DateTimeParsePipe(b3.source(), b3.expression(), b3.left(), b3.right(), b3.zoneId(), newPr);
        assertEquals(newB, b3.transformPropertiesOnly(v -> Objects.equals(v, b3.parser()) ? newPr : v, Parser.class));
    
        DateTimeParsePipe b4 = randomInstance();
        ZoneId newZI = randomValueOtherThan(b4.zoneId(), ESTestCase::randomZone);
        newB = new DateTimeParsePipe(b3.source(), b4.expression(), b4.left(), b4.right(), newZI, b4.parser());
        assertEquals(newB, b4.transformPropertiesOnly(v -> Objects.equals(v, b4.zoneId()) ? newZI : v, ZoneId.class));
    }

    @Override
    public void testReplaceChildren() {
        DateTimeParsePipe b = randomInstance();
        Pipe newLeft = pipe(((Expression) randomValueOtherThan(b.left(), FunctionTestUtils::randomDatetimeLiteral)));
        Pipe newRight = pipe(((Expression) randomValueOtherThan(b.right(), FunctionTestUtils::randomStringLiteral)));
        DateTimeParsePipe newB = new DateTimeParsePipe(
                b.source(), 
                b.expression(), 
                b.left(), 
                b.right(), 
                b.zoneId(), 
                b.parser());
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
    protected DateTimeParsePipe mutate(DateTimeParsePipe instance) {
        List<Function<DateTimeParsePipe, DateTimeParsePipe>> randoms = new ArrayList<>();
        randoms.add(
            f -> new DateTimeParsePipe(
                f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), FunctionTestUtils::randomDatetimeLiteral))),
                f.right(),
                f.zoneId(),
                f.parser()
            )
        );
        randoms.add(
            f -> new DateTimeParsePipe(
                f.source(),
                f.expression(),
                f.left(),
                pipe(((Expression) randomValueOtherThan(f.right(), FunctionTestUtils::randomStringLiteral))),
                f.zoneId(), 
                f.parser()
            )
        );
        randoms.add(
            f -> new DateTimeParsePipe(
                f.source(),
                f.expression(),
                f.left(),
                f.right(),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone),
                f.parser()
            )
        );
        randoms.add(
            f -> new DateTimeParsePipe(
                f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), FunctionTestUtils::randomDatetimeLiteral))),
                pipe(((Expression) randomValueOtherThan(f.right(), FunctionTestUtils::randomStringLiteral))),
                randomValueOtherThan(f.zoneId(), ESTestCase::randomZone), 
                f.parser()
            )
        );
        randoms.add(
            f -> new DateTimeParsePipe(
                f.source(),
                f.expression(),
                f.left(),
                f.right(),
                f.zoneId(),
                randomValueOtherThan(f.parser(), () -> randomFrom(Parser.values()))
            )
        );

        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected DateTimeParsePipe copy(DateTimeParsePipe instance) {
        return new DateTimeParsePipe(
                instance.source(), 
                instance.expression(), 
                instance.left(), 
                instance.right(), 
                instance.zoneId(),
                instance.parser());
    }
}
