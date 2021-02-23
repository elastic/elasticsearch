/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.function;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.expression.UnresolvedAttributeTests.randomUnresolvedAttribute;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;

public class UnresolvedFunctionTests extends AbstractNodeTestCase<UnresolvedFunction, Expression> {

    public UnresolvedFunction randomUnresolvedFunction() {
        /* Pick an UnresolvedFunction where the name and the
         * message don't happen to be the same String. If they
         * matched then transform would get them confused. */
        Source source = randomSource();
        String name = randomAlphaOfLength(5);
        FunctionResolutionStrategy resolutionStrategy = randomFrom(resolutionStrategies());
        List<Expression> args = randomFunctionArgs();
        boolean analyzed = randomBoolean();
        String unresolvedMessage = randomUnresolvedMessage();
        return new UnresolvedFunction(source, name, resolutionStrategy, args, analyzed, unresolvedMessage);
    }

    protected List<FunctionResolutionStrategy> resolutionStrategies() {
        return asList(FunctionResolutionStrategy.DEFAULT, new FunctionResolutionStrategy() {
        });
    }

    private static List<Expression> randomFunctionArgs() {
        // At this point we only support functions with 0, 1, or 2 arguments.
        Supplier<List<Expression>> option = randomFrom(asList(
            Collections::emptyList,
            () -> singletonList(randomUnresolvedAttribute()),
            () -> asList(randomUnresolvedAttribute(), randomUnresolvedAttribute())
        ));
        return option.get();
    }

    /**
     * Pick a random value for the unresolved message.
     * It is important that this value is not the same
     * as the value for the name for tests like the {@link #testTransform}
     * and for general ease of reading.
     */
    private static String randomUnresolvedMessage() {
        return randomBoolean() ? null : randomAlphaOfLength(6);
    }

    @Override
    protected UnresolvedFunction randomInstance() {
        return randomUnresolvedFunction();
    }

    @Override
    protected UnresolvedFunction mutate(UnresolvedFunction uf) {
        Supplier<UnresolvedFunction> option = randomFrom(asList(
            () -> new UnresolvedFunction(uf.source(), randomValueOtherThan(uf.name(), () -> randomAlphaOfLength(5)),
                uf.resolutionStrategy(), uf.children(), uf.analyzed(), uf.unresolvedMessage()),
            () -> new UnresolvedFunction(uf.source(), uf.name(),
                randomValueOtherThan(uf.resolutionStrategy(), () -> randomFrom(resolutionStrategies())),
                uf.children(), uf.analyzed(), uf.unresolvedMessage()),
            () -> new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionStrategy(),
                randomValueOtherThan(uf.children(), UnresolvedFunctionTests::randomFunctionArgs),
                uf.analyzed(), uf.unresolvedMessage()),
            () -> new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionStrategy(), uf.children(),
                uf.analyzed() == false, uf.unresolvedMessage()),
            () -> new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionStrategy(), uf.children(),
                uf.analyzed(), randomValueOtherThan(uf.unresolvedMessage(), () -> randomAlphaOfLength(5)))
        ));
        return option.get();
    }

    @Override
    protected UnresolvedFunction copy(UnresolvedFunction uf) {
        return new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionStrategy(), uf.children(),
            uf.analyzed(), uf.unresolvedMessage());
    }

    @Override
    public void testTransform() {
        UnresolvedFunction uf = randomUnresolvedFunction();

        String newName = randomValueOtherThan(uf.name(), () -> randomAlphaOfLength(5));
        assertEquals(new UnresolvedFunction(uf.source(), newName, uf.resolutionStrategy(), uf.children(),
                uf.analyzed(), uf.unresolvedMessage()),
            uf.transformPropertiesOnly(Object.class, p -> Objects.equals(p, uf.name()) ? newName : p));
        FunctionResolutionStrategy newResolution = randomValueOtherThan(uf.resolutionStrategy(),
            () -> randomFrom(resolutionStrategies()));
        assertEquals(new UnresolvedFunction(uf.source(), uf.name(), newResolution, uf.children(),
                uf.analyzed(), uf.unresolvedMessage()),
            uf.transformPropertiesOnly(Object.class, p -> Objects.equals(p, uf.resolutionStrategy()) ? newResolution : p));
        String newUnresolvedMessage = randomValueOtherThan(uf.unresolvedMessage(), UnresolvedFunctionTests::randomUnresolvedMessage);
        assertEquals(new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionStrategy(), uf.children(),
                uf.analyzed(), newUnresolvedMessage),
            uf.transformPropertiesOnly(Object.class, p -> Objects.equals(p, uf.unresolvedMessage()) ? newUnresolvedMessage : p));

        assertEquals(new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionStrategy(), uf.children(),
                uf.analyzed() == false, uf.unresolvedMessage()),
            uf.transformPropertiesOnly(Object.class, p -> Objects.equals(p, uf.analyzed()) ? uf.analyzed() == false : p));

    }

    @Override
    public void testReplaceChildren() {
        UnresolvedFunction uf = randomUnresolvedFunction();

        List<Expression> newChildren = randomValueOtherThan(uf.children(), UnresolvedFunctionTests::randomFunctionArgs);
        assertEquals(new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionStrategy(), newChildren,
                uf.analyzed(), uf.unresolvedMessage()),
            uf.replaceChildren(newChildren));
    }
}
