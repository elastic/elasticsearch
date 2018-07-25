/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.BinaryProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringNumericProcessor.BinaryStringNumericOperation;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomIntLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions.toProcessorDefinition;
import static org.elasticsearch.xpack.sql.tree.LocationTests.randomLocation;

public class BinaryStringNumericProcessorDefinitionTests 
        extends AbstractNodeTestCase<BinaryStringNumericProcessorDefinition, ProcessorDefinition> {

    @Override
    protected BinaryStringNumericProcessorDefinition randomInstance() {
        return randomBinaryStringNumericProcessorDefinition();
    }
    
    private Expression randomBinaryStringNumericExpression() {        
        return randomBinaryStringNumericProcessorDefinition().expression();
    }
    
    private BinaryStringNumericOperation randomBinaryStringNumericOperation() {        
        return randomBinaryStringNumericProcessorDefinition().operation();
    }
    
    public static BinaryStringNumericProcessorDefinition randomBinaryStringNumericProcessorDefinition() {
        List<ProcessorDefinition> functions = new ArrayList<>();
        functions.add(new Left(randomLocation(), randomStringLiteral(), randomIntLiteral()).makeProcessorDefinition());
        functions.add(new Right(randomLocation(), randomStringLiteral(), randomIntLiteral()).makeProcessorDefinition());
        functions.add(new Repeat(randomLocation(), randomStringLiteral(), randomIntLiteral()).makeProcessorDefinition());
        
        return (BinaryStringNumericProcessorDefinition) randomFrom(functions);
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (location, expression, operation), 
        // skipping the children (the two parameters of the binary function) which are tested separately
        BinaryStringNumericProcessorDefinition b1 = randomInstance();
        
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomBinaryStringNumericExpression());
        BinaryStringNumericProcessorDefinition newB = new BinaryStringNumericProcessorDefinition(
                b1.location(), 
                newExpression,
                b1.left(), 
                b1.right(), 
                b1.operation());
        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        BinaryStringNumericProcessorDefinition b2 = randomInstance();
        BinaryStringNumericOperation newOp = randomValueOtherThan(b2.operation(), () -> randomBinaryStringNumericOperation());
        newB = new BinaryStringNumericProcessorDefinition(
                b2.location(), 
                b2.expression(),
                b2.left(), 
                b2.right(), 
                newOp);
        assertEquals(newB, 
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.operation()) ? newOp : v, BinaryStringNumericOperation.class));
        
        BinaryStringNumericProcessorDefinition b3 = randomInstance();
        Location newLoc = randomValueOtherThan(b3.location(), () -> randomLocation());
        newB = new BinaryStringNumericProcessorDefinition(
                newLoc, 
                b3.expression(),
                b3.left(), 
                b3.right(), 
                b3.operation());
        assertEquals(newB, 
                b3.transformPropertiesOnly(v -> Objects.equals(v, b3.location()) ? newLoc : v, Location.class));
    }

    @Override
    public void testReplaceChildren() {
        BinaryStringNumericProcessorDefinition b = randomInstance();
        ProcessorDefinition newLeft = toProcessorDefinition((Expression) randomValueOtherThan(b.left(), () -> randomStringLiteral()));
        ProcessorDefinition newRight = toProcessorDefinition((Expression) randomValueOtherThan(b.right(), () -> randomIntLiteral()));
        BinaryStringNumericProcessorDefinition newB = 
                new BinaryStringNumericProcessorDefinition(b.location(), b.expression(), b.left(), b.right(), b.operation());
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
    protected BinaryStringNumericProcessorDefinition mutate(BinaryStringNumericProcessorDefinition instance) {
        List<Function<BinaryStringNumericProcessorDefinition, BinaryStringNumericProcessorDefinition>> randoms = new ArrayList<>();
        randoms.add(f -> new BinaryStringNumericProcessorDefinition(f.location(), 
                f.expression(), 
                toProcessorDefinition((Expression) randomValueOtherThan(f.left(), () -> randomStringLiteral())),
                f.right(),
                f.operation()));
        randoms.add(f -> new BinaryStringNumericProcessorDefinition(f.location(), 
                f.expression(), 
                f.left(),
                toProcessorDefinition((Expression) randomValueOtherThan(f.right(), () -> randomIntLiteral())),
                f.operation()));
        randoms.add(f -> new BinaryStringNumericProcessorDefinition(f.location(), 
                f.expression(), 
                toProcessorDefinition((Expression) randomValueOtherThan(f.left(), () -> randomStringLiteral())),
                toProcessorDefinition((Expression) randomValueOtherThan(f.right(), () -> randomIntLiteral())),
                f.operation()));
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected BinaryStringNumericProcessorDefinition copy(BinaryStringNumericProcessorDefinition instance) {
        return new BinaryStringNumericProcessorDefinition(instance.location(),
                instance.expression(),
                instance.left(),
                instance.right(),
                instance.operation());
    }
}
