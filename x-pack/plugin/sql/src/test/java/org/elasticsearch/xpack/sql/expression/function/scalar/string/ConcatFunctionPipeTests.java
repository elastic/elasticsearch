/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.expression.Expressions.pipe;
import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.tree.LocationTests.randomLocation;

public class ConcatFunctionPipeTests extends AbstractNodeTestCase<ConcatFunctionPipe, Pipe> {

    @Override
    protected ConcatFunctionPipe randomInstance() {
        return randomConcatFunctionPipe();
    }
    
    private Expression randomConcatFunctionExpression() {
        return randomConcatFunctionPipe().expression();
    }
    
    public static ConcatFunctionPipe randomConcatFunctionPipe() {
        return (ConcatFunctionPipe) new Concat(
                randomLocation(),
                randomStringLiteral(),
                randomStringLiteral())
                .makePipe();
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (location, expression),
        // skipping the children (the two parameters of the binary function) which are tested separately
        ConcatFunctionPipe b1 = randomInstance();
        
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomConcatFunctionExpression());
        ConcatFunctionPipe newB = new ConcatFunctionPipe(
                b1.location(),
                newExpression,
                b1.left(),
                b1.right());
        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        ConcatFunctionPipe b2 = randomInstance();
        Location newLoc = randomValueOtherThan(b2.location(), () -> randomLocation());
        newB = new ConcatFunctionPipe(
                newLoc,
                b2.expression(),
                b2.left(),
                b2.right());
        assertEquals(newB,
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.location()) ? newLoc : v, Location.class));
    }

    @Override
    public void testReplaceChildren() {
        ConcatFunctionPipe b = randomInstance();
        Pipe newLeft = pipe(((Expression) randomValueOtherThan(b.left(), () -> randomStringLiteral())));
        Pipe newRight = pipe(((Expression) randomValueOtherThan(b.right(), () -> randomStringLiteral())));
        ConcatFunctionPipe newB =
                new ConcatFunctionPipe(b.location(), b.expression(), b.left(), b.right());
        BinaryPipe transformed = newB.replaceChildren(newLeft, b.right());
        
        assertEquals(transformed.left(), newLeft);
        assertEquals(transformed.location(), b.location());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.right(), b.right());
        
        transformed = newB.replaceChildren(b.left(), newRight);
        assertEquals(transformed.left(), b.left());
        assertEquals(transformed.location(), b.location());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.right(), newRight);
        
        transformed = newB.replaceChildren(newLeft, newRight);
        assertEquals(transformed.left(), newLeft);
        assertEquals(transformed.location(), b.location());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.right(), newRight);
    }

    @Override
    protected ConcatFunctionPipe mutate(ConcatFunctionPipe instance) {
        List<Function<ConcatFunctionPipe, ConcatFunctionPipe>> randoms = new ArrayList<>();
        randoms.add(f -> new ConcatFunctionPipe(f.location(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), () -> randomStringLiteral()))),
                f.right()));
        randoms.add(f -> new ConcatFunctionPipe(f.location(),
                f.expression(),
                f.left(),
                pipe(((Expression) randomValueOtherThan(f.right(), () -> randomStringLiteral())))));
        randoms.add(f -> new ConcatFunctionPipe(f.location(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.left(), () -> randomStringLiteral()))),
                pipe(((Expression) randomValueOtherThan(f.right(), () -> randomStringLiteral())))));
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected ConcatFunctionPipe copy(ConcatFunctionPipe instance) {
        return new ConcatFunctionPipe(instance.location(),
                instance.expression(),
                instance.left(),
                instance.right());
    }
}
