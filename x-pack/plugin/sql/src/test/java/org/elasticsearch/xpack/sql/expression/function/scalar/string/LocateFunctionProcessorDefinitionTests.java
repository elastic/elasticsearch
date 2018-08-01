/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.Combinations;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomIntLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions.toProcessorDefinition;
import static org.elasticsearch.xpack.sql.tree.LocationTests.randomLocation;

public class LocateFunctionProcessorDefinitionTests extends AbstractNodeTestCase<LocateFunctionProcessorDefinition, ProcessorDefinition> {

    @Override
    protected LocateFunctionProcessorDefinition randomInstance() {
        return randomLocateFunctionProcessorDefinition();
    }
    
    private Expression randomLocateFunctionExpression() {        
        return randomLocateFunctionProcessorDefinition().expression();
    }
    
    public static LocateFunctionProcessorDefinition randomLocateFunctionProcessorDefinition() {
        return (LocateFunctionProcessorDefinition) (new Locate(randomLocation(), 
                            randomStringLiteral(), 
                            randomStringLiteral(),
                            frequently() ? randomIntLiteral() : null)
                .makeProcessorDefinition());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (location, expression), 
        // skipping the children (the two parameters of the binary function) which are tested separately
        LocateFunctionProcessorDefinition b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomLocateFunctionExpression());
        LocateFunctionProcessorDefinition newB;
        if (b1.start() == null) {
            newB = new LocateFunctionProcessorDefinition(
                    b1.location(), 
                    newExpression,
                    b1.pattern(), 
                    b1.source());
        } else {
            newB = new LocateFunctionProcessorDefinition(
                b1.location(), 
                newExpression,
                b1.pattern(), 
                b1.source(),
                b1.start());
        }
        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        LocateFunctionProcessorDefinition b2 = randomInstance();
        Location newLoc = randomValueOtherThan(b2.location(), () -> randomLocation());
        if (b2.start() == null) {
            newB = new LocateFunctionProcessorDefinition(
                    newLoc, 
                    b2.expression(),
                    b2.pattern(), 
                    b2.source());
        } else {
            newB = new LocateFunctionProcessorDefinition(
                    newLoc, 
                    b2.expression(),
                    b2.pattern(), 
                    b2.source(),
                    b2.start());
        }
        assertEquals(newB, 
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.location()) ? newLoc : v, Location.class));
    }

    @Override
    public void testReplaceChildren() {
        LocateFunctionProcessorDefinition b = randomInstance();
        ProcessorDefinition newPattern = toProcessorDefinition((Expression) randomValueOtherThan(b.pattern(), () -> randomStringLiteral()));
        ProcessorDefinition newSource = toProcessorDefinition((Expression) randomValueOtherThan(b.source(), () -> randomStringLiteral()));
        ProcessorDefinition newStart;
        
        LocateFunctionProcessorDefinition newB;
        if (b.start() == null) {
            newB = new LocateFunctionProcessorDefinition(b.location(), b.expression(), b.pattern(), b.source());
            newStart = null;
        }
        else {
            newB = new LocateFunctionProcessorDefinition(b.location(), b.expression(), b.pattern(), b.source(), b.start());
            newStart = toProcessorDefinition((Expression) randomValueOtherThan(b.start(), () -> randomIntLiteral()));
        }
        LocateFunctionProcessorDefinition transformed = null;
        
        // generate all the combinations of possible children modifications and test all of them
        for(int i = 1; i < 4; i++) {
            for(BitSet comb : new Combinations(3, i)) {
                transformed = (LocateFunctionProcessorDefinition) newB.replaceChildren(
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
    protected LocateFunctionProcessorDefinition mutate(LocateFunctionProcessorDefinition instance) {
        List<Function<LocateFunctionProcessorDefinition, LocateFunctionProcessorDefinition>> randoms = new ArrayList<>();
        if (instance.start() == null) {
            for(int i = 1; i < 3; i++) {
                for(BitSet comb : new Combinations(2, i)) {
                    randoms.add(f -> new LocateFunctionProcessorDefinition(f.location(), 
                            f.expression(), 
                            comb.get(0) ? toProcessorDefinition((Expression) randomValueOtherThan(f.pattern(),
                                    () -> randomStringLiteral())) : f.pattern(),
                            comb.get(1) ? toProcessorDefinition((Expression) randomValueOtherThan(f.source(),
                                    () -> randomStringLiteral())) : f.source()));
                }
            }
        } else {
            for(int i = 1; i < 4; i++) {
                for(BitSet comb : new Combinations(3, i)) {
                    randoms.add(f -> new LocateFunctionProcessorDefinition(f.location(), 
                            f.expression(), 
                            comb.get(0) ? toProcessorDefinition((Expression) randomValueOtherThan(f.pattern(),
                                    () -> randomStringLiteral())) : f.pattern(),
                            comb.get(1) ? toProcessorDefinition((Expression) randomValueOtherThan(f.source(),
                                    () -> randomStringLiteral())) : f.source(),
                            comb.get(2) ? toProcessorDefinition((Expression) randomValueOtherThan(f.start(),
                                    () -> randomIntLiteral())) : f.start()));
                }
            }
        }
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected LocateFunctionProcessorDefinition copy(LocateFunctionProcessorDefinition instance) {
        return instance.start() == null ?
                new LocateFunctionProcessorDefinition(instance.location(),
                        instance.expression(),
                        instance.pattern(),
                        instance.source())
                :
                new LocateFunctionProcessorDefinition(instance.location(),
                        instance.expression(),
                        instance.pattern(),
                        instance.source(),
                        instance.start());
    }
}
