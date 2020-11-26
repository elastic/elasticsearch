/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.Expressions.pipe;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;

public class CIDRMatchFunctionPipeTests extends AbstractNodeTestCase<CIDRMatchFunctionPipe, Pipe> {

    @Override
    protected CIDRMatchFunctionPipe randomInstance() {
        return randomCIDRMatchFunctionPipe();
    }

    private Expression randomCIDRMatchFunctionExpression() {
        return randomCIDRMatchFunctionPipe().expression();
    }

    public static CIDRMatchFunctionPipe randomCIDRMatchFunctionPipe() {
        int size = randomIntBetween(2, 10);
        List<Expression> addresses = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            addresses.add(randomStringLiteral());
        }
        return (CIDRMatchFunctionPipe) (new CIDRMatch(randomSource(), randomStringLiteral(), addresses).makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (input, addresses) which are tested separately
        CIDRMatchFunctionPipe b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomCIDRMatchFunctionExpression());
        CIDRMatchFunctionPipe newB = new CIDRMatchFunctionPipe(
            b1.source(),
            newExpression,
            b1.input(),
            b1.addresses());

        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        CIDRMatchFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new CIDRMatchFunctionPipe(
            newLoc,
            b2.expression(),
            b2.input(),
            b2.addresses());

        assertEquals(newB, b2.transformPropertiesOnly(v -> Objects.equals(v, b2.source()) ? newLoc : v, Source.class));
    }

    @Override
    public void testReplaceChildren() {
        CIDRMatchFunctionPipe b = randomInstance();
        Pipe newInput = randomValueOtherThan(b.input(), () -> pipe(randomStringLiteral()));
        List<Pipe> newAddresses = mutateOneAddress(b.addresses());
        
        CIDRMatchFunctionPipe newB = new CIDRMatchFunctionPipe(b.source(), b.expression(), b.input(), b.addresses());
        CIDRMatchFunctionPipe transformed = newB.replaceChildren(newInput, b.addresses());
        
        assertEquals(transformed.input(), newInput);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.addresses(), b.addresses());
        
        transformed = newB.replaceChildren(b.input(), newAddresses);
        assertEquals(transformed.input(), b.input());
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.addresses(), newAddresses);
        
        transformed = newB.replaceChildren(newInput, newAddresses);
        assertEquals(transformed.input(), newInput);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.addresses(), newAddresses);
    }

    @Override
    protected CIDRMatchFunctionPipe mutate(CIDRMatchFunctionPipe instance) {
        List<Function<CIDRMatchFunctionPipe, CIDRMatchFunctionPipe>> randoms = new ArrayList<>();
        randoms.add(f -> new CIDRMatchFunctionPipe(f.source(),
                f.expression(),
                randomValueOtherThan(f.input(), () -> pipe(randomStringLiteral())),
                f.addresses()));
        randoms.add(f -> new CIDRMatchFunctionPipe(f.source(),
                f.expression(),
                f.input(),
                mutateOneAddress(f.addresses())));
        randoms.add(f -> new CIDRMatchFunctionPipe(f.source(),
                f.expression(),
                randomValueOtherThan(f.input(), () -> pipe(randomStringLiteral())),
                mutateOneAddress(f.addresses())));
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected CIDRMatchFunctionPipe copy(CIDRMatchFunctionPipe instance) {
        return new CIDRMatchFunctionPipe(instance.source(), instance.expression(), instance.input(), instance.addresses());
    }
    
    private List<Pipe> mutateOneAddress(List<Pipe> oldAddresses) {
        int size = oldAddresses.size();
        ArrayList<Pipe> newAddresses = new ArrayList<>(size);
        
        int index = randomIntBetween(0, size - 1);
        for (int i = 0; i < size; i++) {
            Pipe p = oldAddresses.get(i);
            newAddresses.add(i != index ? p : randomValueOtherThan(p, () -> pipe(randomStringLiteral())));
        }
        return newAddresses;
    }
}
