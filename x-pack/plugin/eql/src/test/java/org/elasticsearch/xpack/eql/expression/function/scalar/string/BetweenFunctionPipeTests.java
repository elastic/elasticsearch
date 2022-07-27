/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

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
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomBooleanLiteral;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;

public class BetweenFunctionPipeTests extends AbstractNodeTestCase<BetweenFunctionPipe, Pipe> {

    @Override
    protected BetweenFunctionPipe randomInstance() {
        return randomBetweenFunctionPipe();
    }

    private Expression randomBetweenFunctionExpression() {
        return randomBetweenFunctionPipe().expression();
    }

    public static BetweenFunctionPipe randomBetweenFunctionPipe() {
        return (BetweenFunctionPipe) (new Between(
            randomSource(),
            randomStringLiteral(),
            randomStringLiteral(),
            randomStringLiteral(),
            randomBooleanLiteral(),
            randomBoolean()
        ).makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (input, left, right, greedy, caseSensitive) which are tested separately
        BetweenFunctionPipe b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomBetweenFunctionExpression());
        BetweenFunctionPipe newB = new BetweenFunctionPipe(
            b1.source(),
            newExpression,
            b1.input(),
            b1.left(),
            b1.right(),
            b1.greedy(),
            b1.isCaseInsensitive()
        );

        assertEquals(newB, b1.transformPropertiesOnly(Expression.class, v -> Objects.equals(v, b1.expression()) ? newExpression : v));

        BetweenFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new BetweenFunctionPipe(newLoc, b2.expression(), b2.input(), b2.left(), b2.right(), b2.greedy(), b2.isCaseInsensitive());

        assertEquals(newB, b2.transformPropertiesOnly(Source.class, v -> Objects.equals(v, b2.source()) ? newLoc : v));
    }

    @Override
    public void testReplaceChildren() {
        BetweenFunctionPipe b = randomInstance();
        Pipe newInput = randomValueOtherThan(b.input(), () -> pipe(randomStringLiteral()));
        Pipe newLeft = randomValueOtherThan(b.left(), () -> pipe(randomStringLiteral()));
        Pipe newRight = randomValueOtherThan(b.right(), () -> pipe(randomStringLiteral()));
        Pipe newGreedy = b.greedy() == null ? null : randomValueOtherThan(b.greedy(), () -> pipe(randomBooleanLiteral()));
        boolean newCaseSensitive = b.isCaseInsensitive() == false;

        BetweenFunctionPipe newB = new BetweenFunctionPipe(
            b.source(),
            b.expression(),
            b.input(),
            b.left(),
            b.right(),
            b.greedy(),
            newCaseSensitive
        );
        BetweenFunctionPipe transformed = null;

        // generate all the combinations of possible children modifications and test all of them
        for (int i = 1; i < 4; i++) {
            for (BitSet comb : new Combinations(5, i)) {
                Pipe tempNewGreedy = b.greedy() == null ? b.greedy() : (comb.get(3) ? newGreedy : b.greedy());
                transformed = (BetweenFunctionPipe) newB.replaceChildren(
                    comb.get(0) ? newInput : b.input(),
                    comb.get(1) ? newLeft : b.left(),
                    comb.get(2) ? newRight : b.right(),
                    tempNewGreedy
                );

                assertEquals(transformed.input(), comb.get(0) ? newInput : b.input());
                assertEquals(transformed.left(), comb.get(1) ? newLeft : b.left());
                assertEquals(transformed.right(), comb.get(2) ? newRight : b.right());
                assertEquals(transformed.greedy(), tempNewGreedy);
                assertEquals(transformed.isCaseInsensitive(), newCaseSensitive);
                assertEquals(transformed.expression(), b.expression());
                assertEquals(transformed.source(), b.source());
            }
        }
    }

    @Override
    protected BetweenFunctionPipe mutate(BetweenFunctionPipe instance) {
        List<Function<BetweenFunctionPipe, BetweenFunctionPipe>> randoms = new ArrayList<>();
        if (instance.greedy() == null) {
            for (int i = 1; i < 5; i++) {
                for (BitSet comb : new Combinations(4, i)) {
                    randoms.add(
                        f -> new BetweenFunctionPipe(
                            f.source(),
                            f.expression(),
                            comb.get(0) ? randomValueOtherThan(f.input(), () -> pipe(randomStringLiteral())) : f.input(),
                            comb.get(1) ? randomValueOtherThan(f.left(), () -> pipe(randomStringLiteral())) : f.left(),
                            comb.get(2) ? randomValueOtherThan(f.right(), () -> pipe(randomStringLiteral())) : f.right(),
                            null,
                            comb.get(4) ? f.isCaseInsensitive() == false : f.isCaseInsensitive()
                        )
                    );
                }
            }
        } else {
            for (int i = 1; i < 6; i++) {
                for (BitSet comb : new Combinations(5, i)) {
                    randoms.add(
                        f -> new BetweenFunctionPipe(
                            f.source(),
                            f.expression(),
                            comb.get(0) ? randomValueOtherThan(f.input(), () -> pipe(randomStringLiteral())) : f.input(),
                            comb.get(1) ? randomValueOtherThan(f.left(), () -> pipe(randomStringLiteral())) : f.left(),
                            comb.get(2) ? randomValueOtherThan(f.right(), () -> pipe(randomStringLiteral())) : f.right(),
                            comb.get(3) ? randomValueOtherThan(f.greedy(), () -> pipe(randomBooleanLiteral())) : f.greedy(),
                            comb.get(4) ? f.isCaseInsensitive() == false : f.isCaseInsensitive()
                        )
                    );
                }
            }
        }

        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected BetweenFunctionPipe copy(BetweenFunctionPipe instance) {
        return new BetweenFunctionPipe(
            instance.source(),
            instance.expression(),
            instance.input(),
            instance.left(),
            instance.right(),
            instance.greedy(),
            instance.isCaseInsensitive()
        );
    }
}
