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
                b1.input(),
                b1.pattern(),
                b1.isCaseSensitive());

        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        StartsWithFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new StartsWithFunctionPipe(
                newLoc,
                b2.expression(),
                b2.input(),
                b2.pattern(),
                b2.isCaseSensitive());

        assertEquals(newB,
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.source()) ? newLoc : v, Source.class));
    }

    @Override
    public void testReplaceChildren() {
        StartsWithFunctionPipe b = randomInstance();
        Pipe newInput = randomValueOtherThan(b.input(), () -> pipe(randomStringLiteral()));
        Pipe newPattern = randomValueOtherThan(b.pattern(), () -> pipe(randomStringLiteral()));
        
        StartsWithFunctionPipe newB = new StartsWithFunctionPipe(b.source(), b.expression(), b.input(), b.pattern(), b.isCaseSensitive());
        StartsWithFunctionPipe transformed = (StartsWithFunctionPipe) newB.replaceChildren(newInput, b.pattern());
        assertEquals(transformed.input(), newInput);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.pattern(), b.pattern());
        
        transformed = (StartsWithFunctionPipe) newB.replaceChildren(b.input(), newPattern);
        assertEquals(transformed.input(), b.input());
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.pattern(), newPattern);
        
        transformed = (StartsWithFunctionPipe) newB.replaceChildren(newInput, newPattern);
        assertEquals(transformed.input(), newInput);
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
                        comb.get(0) ? randomValueOtherThan(f.input(), () -> pipe(randomStringLiteral())) : f.input(),
                        comb.get(1) ? randomValueOtherThan(f.pattern(), () -> pipe(randomStringLiteral())) : f.pattern(),
                        comb.get(2) ? randomValueOtherThan(f.isCaseSensitive(), () -> randomBoolean()) : f.isCaseSensitive()));
            }
        }
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected StartsWithFunctionPipe copy(StartsWithFunctionPipe instance) {
        return new StartsWithFunctionPipe(instance.source(),
                        instance.expression(),
                        instance.input(),
                        instance.pattern(),
                        instance.isCaseSensitive());
    }
}