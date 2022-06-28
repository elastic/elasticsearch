/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
        return (ReplaceFunctionPipe) (new Replace(randomSource(), randomStringLiteral(), randomStringLiteral(), randomStringLiteral())
            .makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (the two parameters of the binary function) which are tested separately
        ReplaceFunctionPipe b1 = randomInstance();

        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomReplaceFunctionExpression());
        ReplaceFunctionPipe newB = new ReplaceFunctionPipe(b1.source(), newExpression, b1.input(), b1.pattern(), b1.replacement());
        assertEquals(newB, b1.transformPropertiesOnly(Expression.class, v -> Objects.equals(v, b1.expression()) ? newExpression : v));

        ReplaceFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new ReplaceFunctionPipe(newLoc, b2.expression(), b2.input(), b2.pattern(), b2.replacement());
        assertEquals(newB, b2.transformPropertiesOnly(Source.class, v -> Objects.equals(v, b2.source()) ? newLoc : v));
    }

    @Override
    public void testReplaceChildren() {
        ReplaceFunctionPipe b = randomInstance();
        Pipe newInput = randomValueOtherThan(b.input(), () -> pipe(randomStringLiteral()));
        Pipe newPattern = randomValueOtherThan(b.pattern(), () -> pipe(randomStringLiteral()));
        Pipe newR = randomValueOtherThan(b.replacement(), () -> pipe(randomStringLiteral()));
        ReplaceFunctionPipe newB = new ReplaceFunctionPipe(b.source(), b.expression(), b.input(), b.pattern(), b.replacement());
        ReplaceFunctionPipe transformed = null;

        // generate all the combinations of possible children modifications and test all of them
        for (int i = 1; i < 4; i++) {
            for (BitSet comb : new Combinations(3, i)) {
                transformed = (ReplaceFunctionPipe) newB.replaceChildren(
                    comb.get(0) ? newInput : b.input(),
                    comb.get(1) ? newPattern : b.pattern(),
                    comb.get(2) ? newR : b.replacement()
                );

                assertEquals(transformed.input(), comb.get(0) ? newInput : b.input());
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

        for (int i = 1; i < 4; i++) {
            for (BitSet comb : new Combinations(3, i)) {
                randoms.add(
                    f -> new ReplaceFunctionPipe(
                        f.source(),
                        f.expression(),
                        comb.get(0) ? randomValueOtherThan(f.input(), () -> pipe(randomStringLiteral())) : f.input(),
                        comb.get(1) ? randomValueOtherThan(f.pattern(), () -> pipe(randomStringLiteral())) : f.pattern(),
                        comb.get(2) ? randomValueOtherThan(f.replacement(), () -> pipe(randomStringLiteral())) : f.replacement()
                    )
                );
            }
        }

        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected ReplaceFunctionPipe copy(ReplaceFunctionPipe instance) {
        return new ReplaceFunctionPipe(
            instance.source(),
            instance.expression(),
            instance.input(),
            instance.pattern(),
            instance.replacement()
        );
    }
}
