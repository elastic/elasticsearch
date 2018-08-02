/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.BinaryProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions.toProcessorDefinition;
import static org.elasticsearch.xpack.sql.tree.LocationTests.randomLocation;

public class BinaryStringStringProcessorDefinitionTests 
        extends AbstractNodeTestCase<BinaryStringStringProcessorDefinition, ProcessorDefinition> {

    @Override
    protected BinaryStringStringProcessorDefinition randomInstance() {
        return randomBinaryStringStringProcessorDefinition();
    }
    
    private Expression randomBinaryStringStringExpression() {        
        return randomBinaryStringStringProcessorDefinition().expression();
    }
    
    public static BinaryStringStringProcessorDefinition randomBinaryStringStringProcessorDefinition() {
        List<ProcessorDefinition> functions = new ArrayList<>();
        functions.add(new Position(
                randomLocation(), 
                randomStringLiteral(), 
                randomStringLiteral()
                ).makeProcessorDefinition());
        // if we decide to add DIFFERENCE(string,string) in the future, here we'd add it as well
        return (BinaryStringStringProcessorDefinition) randomFrom(functions);
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (location, expression), 
        // skipping the children (the two parameters of the binary function) which are tested separately
        BinaryStringStringProcessorDefinition b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomBinaryStringStringExpression());
        BinaryStringStringProcessorDefinition newB = new BinaryStringStringProcessorDefinition(
                b1.location(), 
                newExpression,
                b1.left(), 
                b1.right(), 
                b1.operation());
        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        BinaryStringStringProcessorDefinition b2 = randomInstance();
        Location newLoc = randomValueOtherThan(b2.location(), () -> randomLocation());
        newB = new BinaryStringStringProcessorDefinition(
                newLoc, 
                b2.expression(),
                b2.left(), 
                b2.right(), 
                b2.operation());
        assertEquals(newB, 
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.location()) ? newLoc : v, Location.class));
    }

    @Override
    public void testReplaceChildren() {
        BinaryStringStringProcessorDefinition b = randomInstance();
        ProcessorDefinition newLeft = toProcessorDefinition((Expression) randomValueOtherThan(b.left(), () -> randomStringLiteral()));
        ProcessorDefinition newRight = toProcessorDefinition((Expression) randomValueOtherThan(b.right(), () -> randomStringLiteral()));
        BinaryStringStringProcessorDefinition newB = 
                new BinaryStringStringProcessorDefinition(b.location(), b.expression(), b.left(), b.right(), b.operation());
        
        BinaryProcessorDefinition transformed = newB.replaceChildren(newLeft, b.right());
        assertEquals(transformed.left(), newLeft);
        assertEquals(transformed.location(), b.location());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.right(), b.right());
        
        transformed = newB.replaceChildren(b.left(), newRight);
        assertEquals(transformed.left(), b.left());
        assertEquals(transformed.location(), b.location());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.right(), newRight);
        
        transformed = newB.replaceChildren(newLeft, newRight);
        assertEquals(transformed.left(), newLeft);
        assertEquals(transformed.location(), b.location());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.right(), newRight);
    }

    @Override
    protected BinaryStringStringProcessorDefinition mutate(BinaryStringStringProcessorDefinition instance) {
        List<Function<BinaryStringStringProcessorDefinition, BinaryStringStringProcessorDefinition>> randoms = new ArrayList<>();
        randoms.add(f -> new BinaryStringStringProcessorDefinition(f.location(), 
                f.expression(), 
                toProcessorDefinition((Expression) randomValueOtherThan(f.left(), () -> randomStringLiteral())),
                f.right(),
                f.operation()));
        randoms.add(f -> new BinaryStringStringProcessorDefinition(f.location(), 
                f.expression(), 
                f.left(),
                toProcessorDefinition((Expression) randomValueOtherThan(f.right(), () -> randomStringLiteral())),
                f.operation()));
        randoms.add(f -> new BinaryStringStringProcessorDefinition(f.location(), 
                f.expression(), 
                toProcessorDefinition((Expression) randomValueOtherThan(f.left(), () -> randomStringLiteral())),
                toProcessorDefinition((Expression) randomValueOtherThan(f.right(), () -> randomStringLiteral())),
                f.operation()));
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected BinaryStringStringProcessorDefinition copy(BinaryStringStringProcessorDefinition instance) {
        return new BinaryStringStringProcessorDefinition(instance.location(),
                instance.expression(),
                instance.left(),
                instance.right(),
                instance.operation());
    }
}
