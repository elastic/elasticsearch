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

import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions.toProcessorDefinition;
import static org.elasticsearch.xpack.sql.tree.LocationTests.randomLocation;

public class ReplaceFunctionProcessorDefinitionTests extends AbstractNodeTestCase<ReplaceFunctionProcessorDefinition, ProcessorDefinition> {

    @Override
    protected ReplaceFunctionProcessorDefinition randomInstance() {
        return randomReplaceFunctionProcessorDefinition();
    }
    
    private Expression randomReplaceFunctionExpression() {        
        return randomReplaceFunctionProcessorDefinition().expression();
    }
    
    public static ReplaceFunctionProcessorDefinition randomReplaceFunctionProcessorDefinition() {
        return (ReplaceFunctionProcessorDefinition) (new Replace(randomLocation(), 
                            randomStringLiteral(), 
                            randomStringLiteral(),
                            randomStringLiteral())
                .makeProcessorDefinition());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (location, expression), 
        // skipping the children (the two parameters of the binary function) which are tested separately
        ReplaceFunctionProcessorDefinition b1 = randomInstance();
        
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomReplaceFunctionExpression());
        ReplaceFunctionProcessorDefinition newB = new ReplaceFunctionProcessorDefinition(
                b1.location(), 
                newExpression,
                b1.source(), 
                b1.pattern(),
                b1.replacement());
        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        ReplaceFunctionProcessorDefinition b2 = randomInstance();
        Location newLoc = randomValueOtherThan(b2.location(), () -> randomLocation());
        newB = new ReplaceFunctionProcessorDefinition(
                newLoc, 
                b2.expression(),
                b2.source(), 
                b2.pattern(),
                b2.replacement());
        assertEquals(newB, 
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.location()) ? newLoc : v, Location.class));
    }

    @Override
    public void testReplaceChildren() {
        ReplaceFunctionProcessorDefinition b = randomInstance();
        ProcessorDefinition newSource = toProcessorDefinition((Expression) randomValueOtherThan(b.source(), () -> randomStringLiteral()));
        ProcessorDefinition newPattern = toProcessorDefinition((Expression) randomValueOtherThan(b.pattern(), () -> randomStringLiteral()));
        ProcessorDefinition newR = toProcessorDefinition((Expression) randomValueOtherThan(b.replacement(), () -> randomStringLiteral()));
        ReplaceFunctionProcessorDefinition newB = 
                new ReplaceFunctionProcessorDefinition(b.location(), b.expression(), b.source(), b.pattern(), b.replacement());
        ReplaceFunctionProcessorDefinition transformed = null;
        
        // generate all the combinations of possible children modifications and test all of them
        for(int i = 1; i < 4; i++) {
            for(BitSet comb : new Combinations(3, i)) {
                transformed = (ReplaceFunctionProcessorDefinition) newB.replaceChildren(
                        comb.get(0) ? newSource : b.source(),
                        comb.get(1) ? newPattern : b.pattern(),
                        comb.get(2) ? newR : b.replacement());
                
                assertEquals(transformed.source(), comb.get(0) ? newSource : b.source());
                assertEquals(transformed.pattern(), comb.get(1) ? newPattern : b.pattern());
                assertEquals(transformed.replacement(), comb.get(2) ? newR : b.replacement());
                assertEquals(transformed.expression(), b.expression());
                assertEquals(transformed.location(), b.location());
            }
        }
    }

    @Override
    protected ReplaceFunctionProcessorDefinition mutate(ReplaceFunctionProcessorDefinition instance) {
        List<Function<ReplaceFunctionProcessorDefinition, ReplaceFunctionProcessorDefinition>> randoms = new ArrayList<>();
        
        for(int i = 1; i < 4; i++) {
            for(BitSet comb : new Combinations(3, i)) {
                randoms.add(f -> new ReplaceFunctionProcessorDefinition(f.location(), 
                        f.expression(), 
                        comb.get(0) ? toProcessorDefinition((Expression) randomValueOtherThan(f.source(),
                                () -> randomStringLiteral())) : f.source(),
                        comb.get(1) ? toProcessorDefinition((Expression) randomValueOtherThan(f.pattern(),
                                () -> randomStringLiteral())) : f.pattern(),
                        comb.get(2) ? toProcessorDefinition((Expression) randomValueOtherThan(f.replacement(),
                                () -> randomStringLiteral())) : f.replacement()));
            }
        }
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected ReplaceFunctionProcessorDefinition copy(ReplaceFunctionProcessorDefinition instance) {
        return new ReplaceFunctionProcessorDefinition(instance.location(),
                instance.expression(),
                instance.source(),
                instance.pattern(),
                instance.replacement());
    }
}
