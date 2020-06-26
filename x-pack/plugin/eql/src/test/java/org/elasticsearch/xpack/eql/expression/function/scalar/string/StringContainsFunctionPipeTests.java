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

public class StringContainsFunctionPipeTests extends AbstractNodeTestCase<StringContainsFunctionPipe, Pipe> {

    @Override
    protected StringContainsFunctionPipe randomInstance() {
        return randomStringContainsFunctionPipe();
    }
    
    private Expression randomStringContainsFunctionExpression() {
        return randomStringContainsFunctionPipe().expression();
    }
    
    public static StringContainsFunctionPipe randomStringContainsFunctionPipe() {
        return (StringContainsFunctionPipe) (new StringContains(randomSource(), randomStringLiteral(), randomStringLiteral()).makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (string and substring) which are tested separately
        StringContainsFunctionPipe b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomStringContainsFunctionExpression());
        StringContainsFunctionPipe newB = new StringContainsFunctionPipe(
                b1.source(),
                newExpression,
                b1.string(),
                b1.substring());
        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        StringContainsFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new StringContainsFunctionPipe(
                newLoc,
                b2.expression(),
                b2.string(),
                b2.substring());
        assertEquals(newB,
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.source()) ? newLoc : v, Source.class));
    }

    @Override
    public void testReplaceChildren() {
        StringContainsFunctionPipe b = randomInstance();
        Pipe newString = pipe(((Expression) randomValueOtherThan(b.string(), () -> randomStringLiteral())));
        Pipe newSubstring = pipe(((Expression) randomValueOtherThan(b.substring(), () -> randomStringLiteral())));
        StringContainsFunctionPipe newB =
                new StringContainsFunctionPipe(b.source(), b.expression(), b.string(), b.substring());
        
        StringContainsFunctionPipe transformed = newB.replaceChildren(newString, b.substring());
        assertEquals(transformed.string(), newString);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.substring(), b.substring());
        
        transformed = newB.replaceChildren(b.string(), newSubstring);
        assertEquals(transformed.string(), b.string());
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.substring(), newSubstring);
        
        transformed = newB.replaceChildren(newString, newSubstring);
        assertEquals(transformed.string(), newString);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.substring(), newSubstring);
    }

    @Override
    protected StringContainsFunctionPipe mutate(StringContainsFunctionPipe instance) {
        List<Function<StringContainsFunctionPipe, StringContainsFunctionPipe>> randoms = new ArrayList<>();
        randoms.add(f -> new StringContainsFunctionPipe(f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.string(), () -> randomStringLiteral()))),
                f.substring()));
        randoms.add(f -> new StringContainsFunctionPipe(f.source(),
                f.expression(),
                f.string(),
                pipe(((Expression) randomValueOtherThan(f.substring(), () -> randomStringLiteral())))));
        randoms.add(f -> new StringContainsFunctionPipe(f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.string(), () -> randomStringLiteral()))),
                pipe(((Expression) randomValueOtherThan(f.substring(), () -> randomStringLiteral())))));
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected StringContainsFunctionPipe copy(StringContainsFunctionPipe instance) {
        return new StringContainsFunctionPipe(instance.source(),
                instance.expression(),
                instance.string(),
                instance.substring());
    }
}
