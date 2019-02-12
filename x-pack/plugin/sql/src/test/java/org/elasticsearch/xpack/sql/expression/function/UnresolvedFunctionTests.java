/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.sql.expression.UnresolvedAttributeTests.randomUnresolvedAttribute;
import static org.elasticsearch.xpack.sql.tree.SourceTests.randomSource;

public class UnresolvedFunctionTests extends AbstractNodeTestCase<UnresolvedFunction, Expression> {
    public static UnresolvedFunction randomUnresolvedFunction() {
        /* Pick an UnresolvedFunction where the name and the
         * message don't happen to be the same String. If they
         * matched then transform would get them confused. */
        Source source = randomSource();
        String name = randomAlphaOfLength(5);
        UnresolvedFunction.ResolutionType resolutionType = randomFrom(UnresolvedFunction.ResolutionType.values());
        List<Expression> args = randomFunctionArgs();
        boolean analyzed = randomBoolean();
        String unresolvedMessage = randomUnresolvedMessage();
        return new UnresolvedFunction(source, name, resolutionType, args, analyzed, unresolvedMessage);
    }

    private static List<Expression> randomFunctionArgs() {
        // At this point we only support functions with 0, 1, or 2 arguments.
        Supplier<List<Expression>> option = randomFrom(Arrays.asList(
            Collections::emptyList,
            () -> singletonList(randomUnresolvedAttribute()),
            () -> Arrays.asList(randomUnresolvedAttribute(), randomUnresolvedAttribute())
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
        Supplier<UnresolvedFunction> option = randomFrom(Arrays.asList(
            () -> new UnresolvedFunction(uf.source(), randomValueOtherThan(uf.name(), () -> randomAlphaOfLength(5)),
                    uf.resolutionType(), uf.children(), uf.analyzed(), uf.unresolvedMessage()),
            () -> new UnresolvedFunction(uf.source(), uf.name(),
                    randomValueOtherThan(uf.resolutionType(), () -> randomFrom(UnresolvedFunction.ResolutionType.values())),
                    uf.children(), uf.analyzed(), uf.unresolvedMessage()),
            () -> new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionType(),
                    randomValueOtherThan(uf.children(), UnresolvedFunctionTests::randomFunctionArgs),
                    uf.analyzed(), uf.unresolvedMessage()),
            () -> new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionType(), uf.children(),
                    !uf.analyzed(), uf.unresolvedMessage()),
            () -> new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionType(), uf.children(),
                    uf.analyzed(), randomValueOtherThan(uf.unresolvedMessage(), () -> randomAlphaOfLength(5)))
        ));
        return option.get();
    }

    @Override
    protected UnresolvedFunction copy(UnresolvedFunction uf) {
        return new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionType(), uf.children(),
                uf.analyzed(), uf.unresolvedMessage());
    }

    @Override
    public void testTransform() {
        UnresolvedFunction uf = randomUnresolvedFunction();

        String newName = randomValueOtherThan(uf.name(), () -> randomAlphaOfLength(5));
        assertEquals(new UnresolvedFunction(uf.source(), newName, uf.resolutionType(), uf.children(),
                    uf.analyzed(), uf.unresolvedMessage()),
                uf.transformPropertiesOnly(p -> Objects.equals(p, uf.name()) ? newName : p, Object.class));

        UnresolvedFunction.ResolutionType newResolutionType = randomValueOtherThan(uf.resolutionType(),
                () -> randomFrom(UnresolvedFunction.ResolutionType.values()));
        assertEquals(new UnresolvedFunction(uf.source(), uf.name(), newResolutionType, uf.children(),
                    uf.analyzed(), uf.unresolvedMessage()),
                uf.transformPropertiesOnly(p -> Objects.equals(p, uf.resolutionType()) ? newResolutionType : p, Object.class));

        String newUnresolvedMessage = randomValueOtherThan(uf.unresolvedMessage(),
                UnresolvedFunctionTests::randomUnresolvedMessage);
        assertEquals(new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionType(), uf.children(),
                    uf.analyzed(), newUnresolvedMessage),
                uf.transformPropertiesOnly(p -> Objects.equals(p, uf.unresolvedMessage()) ? newUnresolvedMessage : p, Object.class));

        assertEquals(new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionType(), uf.children(),
                !uf.analyzed(), uf.unresolvedMessage()),
            uf.transformPropertiesOnly(p -> Objects.equals(p, uf.analyzed()) ? !uf.analyzed() : p, Object.class));

    }

    @Override
    public void testReplaceChildren() {
        UnresolvedFunction uf = randomUnresolvedFunction();

        List<Expression> newChildren = randomValueOtherThan(uf.children(), UnresolvedFunctionTests::randomFunctionArgs);
        assertEquals(new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionType(), newChildren,
                    uf.analyzed(), uf.unresolvedMessage()),
                uf.replaceChildren(newChildren));
    }
}
