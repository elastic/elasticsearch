/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.eql.EqlTestUtils;
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

public class EndsWithFunctionPipeTests extends AbstractNodeTestCase<EndsWithFunctionPipe, Pipe> {

    @Override
    protected EndsWithFunctionPipe randomInstance() {
        return randomEndsWithFunctionPipe();
    }
    
    private Expression randomEndsWithFunctionExpression() {
        return randomEndsWithFunctionPipe().expression();
    }
    
    public static EndsWithFunctionPipe randomEndsWithFunctionPipe() {
        return (EndsWithFunctionPipe) (new EndsWith(randomSource(),
                randomStringLiteral(),
                randomStringLiteral(),
                EqlTestUtils.randomConfiguration())
            .makePipe());
    }

    @Override
    public void testTransform() {
        // test transforming only the properties (source, expression),
        // skipping the children (input and pattern) which are tested separately
        EndsWithFunctionPipe b1 = randomInstance();
        Expression newExpression = randomValueOtherThan(b1.expression(), () -> randomEndsWithFunctionExpression());
        EndsWithFunctionPipe newB = new EndsWithFunctionPipe(
                b1.source(),
                newExpression,
                b1.input(),
                b1.pattern(),
                b1.isCaseSensitive());
        assertEquals(newB, b1.transformPropertiesOnly(v -> Objects.equals(v, b1.expression()) ? newExpression : v, Expression.class));
        
        EndsWithFunctionPipe b2 = randomInstance();
        Source newLoc = randomValueOtherThan(b2.source(), () -> randomSource());
        newB = new EndsWithFunctionPipe(
                newLoc,
                b2.expression(),
                b2.input(),
                b2.pattern(),
                b2.isCaseSensitive());
        assertEquals(newB,
                b2.transformPropertiesOnly(v -> Objects.equals(v, b2.source()) ? newLoc : v, Source.class));
    }

    @Override
    public void testReplaceChildren() {
        EndsWithFunctionPipe b = randomInstance();
        Pipe newInput = pipe(((Expression) randomValueOtherThan(b.input(), () -> randomStringLiteral())));
        Pipe newPattern = pipe(((Expression) randomValueOtherThan(b.pattern(), () -> randomStringLiteral())));
        boolean newCaseSensitive = randomValueOtherThan(b.isCaseSensitive(), () -> randomBoolean());
        EndsWithFunctionPipe newB =
                new EndsWithFunctionPipe(b.source(), b.expression(), b.input(), b.pattern(), newCaseSensitive);
        
        EndsWithFunctionPipe transformed = newB.replaceChildren(newInput, b.pattern());
        assertEquals(transformed.input(), newInput);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.pattern(), b.pattern());
        
        transformed = newB.replaceChildren(b.input(), newPattern);
        assertEquals(transformed.input(), b.input());
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.pattern(), newPattern);
        
        transformed = newB.replaceChildren(newInput, newPattern);
        assertEquals(transformed.input(), newInput);
        assertEquals(transformed.source(), b.source());
        assertEquals(transformed.expression(), b.expression());
        assertEquals(transformed.pattern(), newPattern);
    }

    @Override
    protected EndsWithFunctionPipe mutate(EndsWithFunctionPipe instance) {
        List<Function<EndsWithFunctionPipe, EndsWithFunctionPipe>> randoms = new ArrayList<>();
        randoms.add(f -> new EndsWithFunctionPipe(f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.input(), () -> randomStringLiteral()))),
                f.pattern(),
                randomValueOtherThan(f.isCaseSensitive(), () -> randomBoolean())));
        randoms.add(f -> new EndsWithFunctionPipe(f.source(),
                f.expression(),
                f.input(),
                pipe(((Expression) randomValueOtherThan(f.pattern(), () -> randomStringLiteral()))),
                randomValueOtherThan(f.isCaseSensitive(), () -> randomBoolean())));
        randoms.add(f -> new EndsWithFunctionPipe(f.source(),
                f.expression(),
                pipe(((Expression) randomValueOtherThan(f.input(), () -> randomStringLiteral()))),
                pipe(((Expression) randomValueOtherThan(f.pattern(), () -> randomStringLiteral()))),
                randomValueOtherThan(f.isCaseSensitive(), () -> randomBoolean())));
        
        return randomFrom(randoms).apply(instance);
    }

    @Override
    protected EndsWithFunctionPipe copy(EndsWithFunctionPipe instance) {
        return new EndsWithFunctionPipe(instance.source(),
                instance.expression(),
                instance.input(),
                instance.pattern(),
                instance.isCaseSensitive());
    }
}
