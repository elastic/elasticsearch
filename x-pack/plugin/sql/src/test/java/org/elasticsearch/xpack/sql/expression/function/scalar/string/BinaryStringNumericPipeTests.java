/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringNumericProcessor.BinaryStringNumericOperation;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.expression.Expressions.pipe;
import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomIntLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.tree.SourceTests.randomSource;

public class BinaryStringNumericPipeTests
        extends AbstractNodeTestCase<BinaryStringNumericPipe, Pipe> {

    @Override
    protected BinaryStringNumericPipe randomInstance() {
        return randomBinaryStringNumericPipe();
    }
    
    private Expression randomBinaryStringNumericExpression() {
        return randomBinaryStringNumericPipe().expression();
    }
    
    private BinaryStringNumericOperation randomBinaryStringNumericOperation() {
        return randomBinaryStringNumericPipe().operation();
    }
    
    public static BinaryStringNumericPipe randomBinaryStringNumericPipe() {
        List<Pipe> functions = new ArrayList<>();
        functions.add(new Left(randomSource(), randomStringLiteral(), randomIntLiteral()).makePipe());
        functions.add(new Right(randomSource(), randomStringLiteral(), randomIntLiteral()).makePipe());
        functions.add(new Repeat(randomSource(), randomStringLiteral(), randomIntLiteral()).makePipe());
        
        return (BinaryStringNumericPipe) randomFrom(functions);
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression, operation),
        // skipping the children (the two parameters of the binary function) which are tested separately
        BinaryStringNumericPipe b1 = randomInstance();
        
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomBinaryStringNumericExpression());
        BinaryStringNumericPipe newB = new BinaryStringNumericPipe(
                b1.source(),
                newExpression,
                b1.left(),
                b1.right(),
                b1.operation());
        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        BinaryStringNumericPipe b2 = randomInstance();
        BinaryStringNumericOperation newOp = randomValueOtherThan(b2.operation(), () -> randomBinaryStringNumericOperation());
        newB = new BinaryStringNumericPipe(
                b2.source(),
                b2.expression(),
                b2.left(),
                b2.right(),
                newOp);
        assertEquals(newB,
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.operation()) ? newOp : v, BinaryStringNumericOperation.class));
        
        BinaryStringNumericPipe b3 = randomInstance();
        Source newLoc = randomValueOtherThan(b3.source(), () -> randomSource());
        newB = new BinaryStringNumericPipe(
                newLoc,
                b3.expression(),
                b3.left(),
                b3.right(),
                b3.operation());
        assertEquals(newB,
                b3.transformPropertiesOnly(v -> Objects.equals(v, b3.source()) ? newLoc : v, Source.class));
    }

    @Override
    public void testReplaceChildren() {
        BinaryStringNumericPipe b = randomInstance();
        Pipe newLeft = pipe(((Expression) randomValueOtherThan(b.left(), () -> randomStringLiteral())));
        Pipe newRight = pipe(((Expression) randomValueOtherThan(b.right(), () -> randomIntLiteral())));
        BinaryStringNumericPipe newB =
                new BinaryStringNumericPipe(b.source(), b.expression(), b.left(), b.right(), b.operation());
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
    protected BinaryStringNumericPipe mutate(BinaryStringNumericPipe instance) {
        List<Function<BinaryStringNumericPipe, BinaryStringNumericPipe>> randoms = new ArrayList<>();
        randoms.add(f -> new BinaryStringNumericPipe(f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), () -> randomStringLiteral()))),
                f.right(),
                f.operation()));
        randoms.add(f -> new BinaryStringNumericPipe(f.source(),
                f.expression(),
                f.left(),
                pipe(((Expression) randomValueOtherThan(f.right(), () -> randomIntLiteral()))),
                f.operation()));
        randoms.add(f -> new BinaryStringNumericPipe(f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), () -> randomStringLiteral()))),
                pipe(((Expression) randomValueOtherThan(f.right(), () -> randomIntLiteral()))),
                f.operation()));
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected BinaryStringNumericPipe copy(BinaryStringNumericPipe instance) {
        return new BinaryStringNumericPipe(instance.source(),
                instance.expression(),
                instance.left(),
                instance.right(),
                instance.operation());
    }
}
