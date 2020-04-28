/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.string;

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

public class ReplaceFunctionPipeTests extends AbstractNodeTestCase<ReplaceFunctionPipe, Pipe> {

    @Override
    protected ReplaceFunctionPipe randomInstance() {
        return randomReplaceFunctionPipe();
    }
    
    private Expression randomReplaceFunctionExpression() {
        return randomReplaceFunctionPipe().expression();
    }
    
    public static ReplaceFunctionPipe randomReplaceFunctionPipe() {
        return (ReplaceFunctionPipe) (new Replace(randomSource(),
                            randomStringLiteral(),
                            randomStringLiteral(),
                            randomStringLiteral())
                .makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (the two parameters of the binary function) which are tested separately
        ReplaceFunctionPipe b1 = randomInstance();
        
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomReplaceFunctionExpression());
        ReplaceFunctionPipe newB = new ReplaceFunctionPipe(
                b1.source(),
                newExpression,
                b1.src(),
                b1.pattern(),
                b1.replacement());
        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        ReplaceFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new ReplaceFunctionPipe(
                newLoc,
                b2.expression(),
                b2.src(),
                b2.pattern(),
                b2.replacement());
        assertEquals(newB,
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.source()) ? newLoc : v, Source.class));
    }

    @Override
    public void testReplaceChildren() {
        ReplaceFunctionPipe b = randomInstance();
        Pipe newSource = pipe(((Expression) randomValueOtherThan(b.source(), () -> randomStringLiteral())));
        Pipe newPattern = pipe(((Expression) randomValueOtherThan(b.pattern(), () -> randomStringLiteral())));
        Pipe newR = pipe(((Expression) randomValueOtherThan(b.replacement(), () -> randomStringLiteral())));
        ReplaceFunctionPipe newB =
                new ReplaceFunctionPipe(b.source(), b.expression(), b.src(), b.pattern(), b.replacement());
        ReplaceFunctionPipe transformed = null;
        
        // generate all the combinations of possible children modifications and test all of them
        for(int i = 1; i < 4; i++) {
            for(BitSet comb : new Combinations(3, i)) {
                transformed = (ReplaceFunctionPipe) newB.replaceChildren(
                        comb.get(0) ? newSource : b.src(),
                        comb.get(1) ? newPattern : b.pattern(),
                        comb.get(2) ? newR : b.replacement());
                
                assertEquals(transformed.src(), comb.get(0) ? newSource : b.src());
                assertEquals(transformed.pattern(), comb.get(1) ? newPattern : b.pattern());
                assertEquals(transformed.replacement(), comb.get(2) ? newR : b.replacement());
                assertEquals(transformed.expression(), b.expression());
                assertEquals(transformed.source(), b.source());
            }
        }
    }

    @Override
    protected ReplaceFunctionPipe mutate(ReplaceFunctionPipe instance) {
        List<Function<ReplaceFunctionPipe, ReplaceFunctionPipe>> randoms = new ArrayList<>();
        
        for(int i = 1; i < 4; i++) {
            for(BitSet comb : new Combinations(3, i)) {
                randoms.add(f -> new ReplaceFunctionPipe(f.source(),
                        f.expression(),
                        comb.get(0) ? pipe(((Expression) randomValueOtherThan(f.src(),
                                () -> randomStringLiteral()))) : f.src(),
                        comb.get(1) ? pipe(((Expression) randomValueOtherThan(f.pattern(),
                                () -> randomStringLiteral()))) : f.pattern(),
                        comb.get(2) ? pipe(((Expression) randomValueOtherThan(f.replacement(),
                                () -> randomStringLiteral()))) : f.replacement()));
            }
        }
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected ReplaceFunctionPipe copy(ReplaceFunctionPipe instance) {
        return new ReplaceFunctionPipe(instance.source(),
                instance.expression(),
                instance.src(),
                instance.pattern(),
                instance.replacement());
    }
}