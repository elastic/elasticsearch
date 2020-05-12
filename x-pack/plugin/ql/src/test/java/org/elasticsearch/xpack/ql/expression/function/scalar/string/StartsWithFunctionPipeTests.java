/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.Combinations;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.Expressions.pipe;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;

public class StartsWithFunctionPipeTests extends AbstractNodeTestCase<StartsWithFunctionPipe, Pipe> {

    @Override
    protected StartsWithFunctionPipe randomInstance() {
        return randomStartsWithFunctionPipe();
    }
    
    private Expression randomStartsWithFunctionExpression() {
        return randomStartsWithFunctionPipe().expression();
    }
    
    public static StartsWithFunctionPipe randomStartsWithFunctionPipe() {
        return (StartsWithFunctionPipe) (new StartsWith(randomSource(),
                            randomStringLiteral(),
                            randomStringLiteral(),
                            TestUtils.randomConfiguration())
                .makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (the two parameters of the binary function) which are tested separately
        StartsWithFunctionPipe b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomStartsWithFunctionExpression());
        StartsWithFunctionPipe newB = new StartsWithFunctionPipe(
            b1.source(),
            newExpression,
            b1.field(),
            b1.pattern(),
            b1.isCaseSensitive());

        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        StartsWithFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new StartsWithFunctionPipe(
                newLoc,
                b2.expression(),
                b2.field(),
                b2.pattern(),
                b2.isCaseSensitive());

        assertEquals(newB,
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.source()) ? newLoc : v, Source.class));
    }

    @Override
    public void testReplaceChildren() {
        StartsWithFunctionPipe b = randomInstance();
        Pipe newField = pipe(((Expression) randomValueOtherThan(b.field(), () -> randomStringLiteral())));
        Pipe newPattern = pipe(((Expression) randomValueOtherThan(b.pattern(), () -> randomStringLiteral())));
        
        StartsWithFunctionPipe newB = new StartsWithFunctionPipe(b.source(), b.expression(), b.field(), b.pattern(), b.isCaseSensitive());
        StartsWithFunctionPipe transformed = (StartsWithFunctionPipe) newB.replaceChildren(newField, b.pattern());
        assertEquals(transformed.field(), newField);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.pattern(), b.pattern());
        
        transformed = (StartsWithFunctionPipe) newB.replaceChildren(b.field(), newPattern);
        assertEquals(transformed.field(), b.field());
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.pattern(), newPattern);
        
        transformed = (StartsWithFunctionPipe) newB.replaceChildren(newField, newPattern);
        assertEquals(transformed.field(), newField);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.pattern(), newPattern);
    }

    @Override
    protected StartsWithFunctionPipe mutate(StartsWithFunctionPipe instance) {
        List<Function<StartsWithFunctionPipe, StartsWithFunctionPipe>> randoms = new ArrayList<>();
        for (int i = 1; i < 4; i++) {
            for (BitSet comb : new Combinations(3, i)) {
                randoms.add(f -> new StartsWithFunctionPipe(f.source(),
                        f.expression(),
                        comb.get(0) ? pipe(((Expression) randomValueOtherThan(f.field(),
                                () -> randomStringLiteral()))) : f.field(),
                        comb.get(1) ? pipe(((Expression) randomValueOtherThan(f.pattern(),
                                () -> randomStringLiteral()))) : f.pattern(),
                        comb.get(2) ? randomValueOtherThan(f.isCaseSensitive(),
                                () -> randomBoolean()) : f.isCaseSensitive()));
            }
        }
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected StartsWithFunctionPipe copy(StartsWithFunctionPipe instance) {
        return new StartsWithFunctionPipe(instance.source(),
                        instance.expression(),
                        instance.field(),
                        instance.pattern(),
                        instance.isCaseSensitive());
    }
}