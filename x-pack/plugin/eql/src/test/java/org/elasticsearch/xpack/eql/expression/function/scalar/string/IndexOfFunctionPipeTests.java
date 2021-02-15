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
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomIntLiteral;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;

public class IndexOfFunctionPipeTests extends AbstractNodeTestCase<IndexOfFunctionPipe, Pipe> {

    @Override
    protected IndexOfFunctionPipe randomInstance() {
        return randomIndexOfFunctionPipe();
    }

    private Expression randomIndexOfFunctionExpression() {
        return randomIndexOfFunctionPipe().expression();
    }

    public static IndexOfFunctionPipe randomIndexOfFunctionPipe() {
        return (IndexOfFunctionPipe) (new IndexOf(randomSource(),
            randomStringLiteral(),
            randomStringLiteral(),
            randomFrom(true, false) ? randomIntLiteral() : null,
            randomBoolean())
                .makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (input, substring, start) which are tested separately
        IndexOfFunctionPipe b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomIndexOfFunctionExpression());
        IndexOfFunctionPipe newB = new IndexOfFunctionPipe(
            b1.source(),
            newExpression,
            b1.input(),
            b1.substring(),
            b1.start(),
            b1.isCaseInsensitive());

        assertEquals(newB, b1.transformPropertiesOnly(Expression.class, v -> Objects.equals(v, b1.expression()) ? newExpression : v));

        IndexOfFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new IndexOfFunctionPipe(
            newLoc,
            b2.expression(),
            b2.input(),
            b2.substring(),
            b2.start(),
            b2.isCaseInsensitive());

        assertEquals(newB, b2.transformPropertiesOnly(Source.class, v -> Objects.equals(v, b2.source()) ? newLoc : v));
    }

    @Override
    public void testReplaceChildren() {
        IndexOfFunctionPipe b = randomInstance();
        Pipe newInput = randomValueOtherThan(b.input(), () -> pipe(randomStringLiteral()));
        Pipe newSubstring = randomValueOtherThan(b.substring(), () -> pipe(randomStringLiteral()));
        Pipe newStart = b.start() == null ? null : randomValueOtherThan(b.start(), () -> pipe(randomIntLiteral()));
        boolean newCaseSensitive = randomValueOtherThan(b.isCaseInsensitive(), () -> randomBoolean());

        IndexOfFunctionPipe newB = new IndexOfFunctionPipe(b.source(), b.expression(), b.input(), b.substring(), b.start(),
            newCaseSensitive);
        IndexOfFunctionPipe transformed = null;

        // generate all the combinations of possible children modifications and test all of them
        for(int i = 1; i < 4; i++) {
            for(BitSet comb : new Combinations(3, i)) {
                Pipe tempNewStart = b.start() == null ? b.start() : (comb.get(2) ? newStart : b.start());
                transformed = newB.replaceChildren(
                        comb.get(0) ? newInput : b.input(),
                        comb.get(1) ? newSubstring : b.substring(),
                        tempNewStart);

                assertEquals(transformed.input(), comb.get(0) ? newInput : b.input());
                assertEquals(transformed.substring(), comb.get(1) ? newSubstring : b.substring());
                assertEquals(transformed.start(), tempNewStart);
                assertEquals(transformed.expression(), b.expression());
                assertEquals(transformed.source(), b.source());
            }
        }
    }

    @Override
    protected IndexOfFunctionPipe mutate(IndexOfFunctionPipe instance) {
        List<Function<IndexOfFunctionPipe, IndexOfFunctionPipe>> randoms = new ArrayList<>();
        if (instance.start() == null) {
            for(int i = 1; i < 3; i++) {
                for(BitSet comb : new Combinations(2, i)) {
                    randoms.add(f -> new IndexOfFunctionPipe(f.source(),
                        f.expression(),
                        comb.get(0) ? randomValueOtherThan(f.input(), () -> pipe(randomStringLiteral())) : f.input(),
                        comb.get(1) ? randomValueOtherThan(f.substring(), () -> pipe(randomStringLiteral())) : f.substring(),
                        null,
                        randomValueOtherThan(f.isCaseInsensitive(), () -> randomBoolean())));
                }
            }
        } else {
            for(int i = 1; i < 4; i++) {
                for(BitSet comb : new Combinations(3, i)) {
                    randoms.add(f -> new IndexOfFunctionPipe(f.source(),
                        f.expression(),
                        comb.get(0) ? randomValueOtherThan(f.input(), () -> pipe(randomStringLiteral())) : f.input(),
                        comb.get(1) ? randomValueOtherThan(f.substring(), () -> pipe(randomStringLiteral())) : f.substring(),
                        comb.get(2) ? randomValueOtherThan(f.start(), () -> pipe(randomIntLiteral())) : f.start(),
                        randomValueOtherThan(f.isCaseInsensitive(), () -> randomBoolean())));
                }
            }
        }

        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected IndexOfFunctionPipe copy(IndexOfFunctionPipe instance) {
        return new IndexOfFunctionPipe(instance.source(),
            instance.expression(),
            instance.input(),
            instance.substring(),
            instance.start(),
            instance.isCaseInsensitive());
    }
}
