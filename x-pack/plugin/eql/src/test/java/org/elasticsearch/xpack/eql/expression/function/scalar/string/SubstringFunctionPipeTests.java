/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

public class SubstringFunctionPipeTests extends AbstractNodeTestCase<SubstringFunctionPipe, Pipe> {

    @Override
    protected SubstringFunctionPipe randomInstance() {
        return randomSubstringFunctionPipe();
    }

    private Expression randomSubstringFunctionExpression() {
        return randomSubstringFunctionPipe().expression();
    }

    public static SubstringFunctionPipe randomSubstringFunctionPipe() {
        return (SubstringFunctionPipe) (new Substring(randomSource(),
                            randomStringLiteral(),
                            randomIntLiteral(),
                            randomFrom(true, false) ? randomIntLiteral() : null)
                .makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (input, start, end) which are tested separately
        SubstringFunctionPipe b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomSubstringFunctionExpression());
        SubstringFunctionPipe newB = new SubstringFunctionPipe(
            b1.source(),
            newExpression,
            b1.input(),
            b1.start(),
            b1.end());

        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        SubstringFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new SubstringFunctionPipe(
            newLoc,
            b2.expression(),
            b2.input(),
            b2.start(),
            b2.end());

        assertEquals(newB, b2.transformPropertiesOnly(v -> Objects.equals(v, b2.source()) ? newLoc : v, Source.class));
    }

    @Override
    public void testReplaceChildren() {
        SubstringFunctionPipe b = randomInstance();
        Pipe newInput = randomValueOtherThan(b.input(), () -> pipe(randomStringLiteral()));
        Pipe newStart = randomValueOtherThan(b.start(), () -> pipe(randomIntLiteral()));
        Pipe newEnd = b.end() == null ? null : randomValueOtherThan(b.end(), () -> pipe(randomIntLiteral()));
        
        SubstringFunctionPipe newB = new SubstringFunctionPipe(b.source(), b.expression(), b.input(), b.start(), b.end());
        SubstringFunctionPipe transformed = null;
        
        // generate all the combinations of possible children modifications and test all of them
        for(int i = 1; i < 4; i++) {
            for(BitSet comb : new Combinations(3, i)) {
                Pipe tempNewEnd = b.end() == null ? b.end() : (comb.get(2) ? newEnd : b.end());
                transformed = newB.replaceChildren(
                        comb.get(0) ? newInput : b.input(),
                        comb.get(1) ? newStart : b.start(),
                        tempNewEnd);
                
                assertEquals(transformed.input(), comb.get(0) ? newInput : b.input());
                assertEquals(transformed.start(), comb.get(1) ? newStart : b.start());
                assertEquals(transformed.end(), tempNewEnd);
                assertEquals(transformed.expression(), b.expression());
                assertEquals(transformed.source(), b.source());
            }
        }
    }

    @Override
    protected SubstringFunctionPipe mutate(SubstringFunctionPipe instance) {
        List<Function<SubstringFunctionPipe, SubstringFunctionPipe>> randoms = new ArrayList<>();
        if (instance.end() == null) {
            for(int i = 1; i < 3; i++) {
                for(BitSet comb : new Combinations(2, i)) {
                    randoms.add(f -> new SubstringFunctionPipe(f.source(),
                            f.expression(),
                            comb.get(0) ? randomValueOtherThan(f.input(), () -> pipe(randomStringLiteral())) : f.input(),
                            comb.get(1) ? randomValueOtherThan(f.start(), () -> pipe(randomIntLiteral())) : f.start(),
                            null));
                }
            }
        } else {
            for(int i = 1; i < 4; i++) {
                for(BitSet comb : new Combinations(3, i)) {
                    randoms.add(f -> new SubstringFunctionPipe(f.source(),
                            f.expression(),
                            comb.get(0) ? randomValueOtherThan(f.input(), () -> pipe(randomStringLiteral())) : f.input(),
                            comb.get(1) ? randomValueOtherThan(f.start(), () -> pipe(randomIntLiteral())) : f.start(),
                            comb.get(2) ? randomValueOtherThan(f.end(), () -> pipe(randomIntLiteral())) : f.end()));
                }
            }
        }
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected SubstringFunctionPipe copy(SubstringFunctionPipe instance) {
        return new SubstringFunctionPipe(instance.source(),
                        instance.expression(),
                        instance.input(),
                        instance.start(),
                        instance.end());
    }
}
