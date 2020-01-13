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
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomIntLiteral;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;

public class InsertFunctionPipeTests extends AbstractNodeTestCase<InsertFunctionPipe, Pipe> {

    @Override
    protected InsertFunctionPipe randomInstance() {
        return randomInsertFunctionPipe();
    }
    
    private Expression randomInsertFunctionExpression() {
        return randomInsertFunctionPipe().expression();
    }
    
    public static InsertFunctionPipe randomInsertFunctionPipe() {
        return (InsertFunctionPipe) (new Insert(randomSource(),
                            randomStringLiteral(),
                            randomIntLiteral(),
                            randomIntLiteral(),
                            randomStringLiteral())
                .makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (the two parameters of the binary function) which are tested separately
        InsertFunctionPipe b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomInsertFunctionExpression());
        InsertFunctionPipe newB = new InsertFunctionPipe(
                b1.source(),
                newExpression,
                b1.src(),
                b1.start(),
                b1.length(),
                b1.replacement());
        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        InsertFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new InsertFunctionPipe(
                newLoc,
                b2.expression(),
                b2.src(),
                b2.start(),
                b2.length(),
                b2.replacement());
        assertEquals(newB,
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.source()) ? newLoc : v, Source.class));
    }

    @Override
    public void testReplaceChildren() {
        InsertFunctionPipe b = randomInstance();
        Pipe newSource = pipe(((Expression) randomValueOtherThan(b.source(), () -> randomStringLiteral())));
        Pipe newStart = pipe(((Expression) randomValueOtherThan(b.start(), () -> randomIntLiteral())));
        Pipe newLength = pipe(((Expression) randomValueOtherThan(b.length(), () -> randomIntLiteral())));
        Pipe newR = pipe(((Expression) randomValueOtherThan(b.replacement(), () -> randomStringLiteral())));
        InsertFunctionPipe newB =
                new InsertFunctionPipe(b.source(), b.expression(), b.src(), b.start(), b.length(), b.replacement());
        InsertFunctionPipe transformed = null;
        
        // generate all the combinations of possible children modifications and test all of them
        for(int i = 1; i < 5; i++) {
            for(BitSet comb : new Combinations(4, i)) {
                transformed = (InsertFunctionPipe) newB.replaceChildren(
                        comb.get(0) ? newSource : b.src(),
                        comb.get(1) ? newStart : b.start(),
                        comb.get(2) ? newLength : b.length(),
                        comb.get(3) ? newR : b.replacement());
                assertEquals(transformed.src(), comb.get(0) ? newSource : b.src());
                assertEquals(transformed.start(), comb.get(1) ? newStart : b.start());
                assertEquals(transformed.length(), comb.get(2) ? newLength : b.length());
                assertEquals(transformed.replacement(), comb.get(3) ? newR : b.replacement());
                assertEquals(transformed.expression(), b.expression());
                assertEquals(transformed.source(), b.source());
            }
        }
    }

    @Override
    protected InsertFunctionPipe mutate(InsertFunctionPipe instance) {
        List<Function<InsertFunctionPipe, InsertFunctionPipe>> randoms = new ArrayList<>();
        
        for(int i = 1; i < 5; i++) {
            for(BitSet comb : new Combinations(4, i)) {
                randoms.add(f -> new InsertFunctionPipe(
                        f.source(),
                        f.expression(),
                        comb.get(0) ? pipe(((Expression) randomValueOtherThan(f.source(),
                                () -> randomStringLiteral()))) : f.src(),
                        comb.get(1) ? pipe(((Expression) randomValueOtherThan(f.start(),
                                () -> randomIntLiteral()))) : f.start(),
                        comb.get(2) ? pipe(((Expression) randomValueOtherThan(f.length(),
                                () -> randomIntLiteral()))): f.length(),
                        comb.get(3) ? pipe(((Expression) randomValueOtherThan(f.replacement(),
                                () -> randomStringLiteral()))) : f.replacement()));
            }
        }

        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected InsertFunctionPipe copy(InsertFunctionPipe instance) {
        return new InsertFunctionPipe(instance.source(),
                instance.expression(),
                instance.src(),
                instance.start(),
                instance.length(),
                instance.replacement());
    }
}