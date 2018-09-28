/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.Combinations;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.expression.Expressions.pipe;
import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomIntLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.tree.LocationTests.randomLocation;

public class LocateFunctionPipeTests extends AbstractNodeTestCase<LocateFunctionPipe, Pipe> {

    @Override
    protected LocateFunctionPipe randomInstance() {
        return randomLocateFunctionPipe();
    }
    
    private Expression randomLocateFunctionExpression() {
        return randomLocateFunctionPipe().expression();
    }
    
    public static LocateFunctionPipe randomLocateFunctionPipe() {
        return (LocateFunctionPipe) (new Locate(randomLocation(),
                            randomStringLiteral(),
                            randomStringLiteral(),
                            randomFrom(true, false) ? randomIntLiteral() : null)
                .makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (location, expression),
        // skipping the children (the two parameters of the binary function) which are tested separately
        LocateFunctionPipe b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomLocateFunctionExpression());
        LocateFunctionPipe newB = new LocateFunctionPipe(
            b1.location(),
            newExpression,
            b1.pattern(),
            b1.source(),
            b1.start());

        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        LocateFunctionPipe b2 = randomInstance();
        Location newLoc = randomValueOtherThan(b2.location(), () -> randomLocation());
        newB = new LocateFunctionPipe(
                newLoc,
                b2.expression(),
                b2.pattern(),
                b2.source(),
                b2.start());

        assertEquals(newB,
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.location()) ? newLoc : v, Location.class));
    }

    @Override
    public void testReplaceChildren() {
        LocateFunctionPipe b = randomInstance();
        Pipe newPattern = pipe(((Expression) randomValueOtherThan(b.pattern(), () -> randomStringLiteral())));
        Pipe newSource = pipe(((Expression) randomValueOtherThan(b.source(), () -> randomStringLiteral())));
        Pipe newStart;
        
        LocateFunctionPipe newB = new LocateFunctionPipe(
                b.location(), b.expression(), b.pattern(), b.source(), b.start());
        newStart = pipe(((Expression) randomValueOtherThan(b.start(), () -> randomIntLiteral())));
        LocateFunctionPipe transformed = null;
        
        // generate all the combinations of possible children modifications and test all of them
        for(int i = 1; i < 4; i++) {
            for(BitSet comb : new Combinations(3, i)) {
                transformed = (LocateFunctionPipe) newB.replaceChildren(
                        comb.get(0) ? newPattern : b.pattern(),
                        comb.get(1) ? newSource : b.source(),
                        comb.get(2) ? newStart : b.start());
                
                assertEquals(transformed.pattern(), comb.get(0) ? newPattern : b.pattern());
                assertEquals(transformed.source(), comb.get(1) ? newSource : b.source());
                assertEquals(transformed.start(), comb.get(2) ? newStart : b.start());
                assertEquals(transformed.expression(), b.expression());
                assertEquals(transformed.location(), b.location());
            }
        }
    }

    @Override
    protected LocateFunctionPipe mutate(LocateFunctionPipe instance) {
        List<Function<LocateFunctionPipe, LocateFunctionPipe>> randoms = new ArrayList<>();
        if (instance.start() == null) {
            for(int i = 1; i < 3; i++) {
                for(BitSet comb : new Combinations(2, i)) {
                    randoms.add(f -> new LocateFunctionPipe(f.location(),
                            f.expression(),
                            comb.get(0) ? pipe(((Expression) randomValueOtherThan(f.pattern(),
                                    () -> randomStringLiteral()))) : f.pattern(),
                            comb.get(1) ? pipe(((Expression) randomValueOtherThan(f.source(),
                                    () -> randomStringLiteral()))) : f.source(),
                                    null));
                }
            }
        } else {
            for(int i = 1; i < 4; i++) {
                for(BitSet comb : new Combinations(3, i)) {
                    randoms.add(f -> new LocateFunctionPipe(f.location(),
                            f.expression(),
                            comb.get(0) ? pipe(((Expression) randomValueOtherThan(f.pattern(),
                                    () -> randomStringLiteral()))) : f.pattern(),
                            comb.get(1) ? pipe(((Expression) randomValueOtherThan(f.source(),
                                    () -> randomStringLiteral()))) : f.source(),
                            comb.get(2) ? pipe(((Expression) randomValueOtherThan(f.start(),
                                    () -> randomIntLiteral()))) : f.start()));
                }
            }
        }
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected LocateFunctionPipe copy(LocateFunctionPipe instance) {
        return new LocateFunctionPipe(instance.location(),
                        instance.expression(),
                        instance.pattern(),
                        instance.source(),
                        instance.start());
    }
}
