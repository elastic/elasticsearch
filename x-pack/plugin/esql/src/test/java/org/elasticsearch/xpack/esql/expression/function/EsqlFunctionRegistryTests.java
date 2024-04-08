/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistryTests;
import org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.tree.SourceTests;

import java.util.Arrays;

import static org.elasticsearch.xpack.ql.TestUtils.randomConfiguration;
import static org.elasticsearch.xpack.ql.expression.function.FunctionRegistry.def;
import static org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy.DEFAULT;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class EsqlFunctionRegistryTests extends ESTestCase {

    public void testFunctionResolving() {
        UnresolvedFunction ur = uf(DEFAULT, mock(Expression.class));
        FunctionRegistry r = new EsqlFunctionRegistry(defineDummyFunction(ur, "dummyfunction", "dummyfunc"));

        // Resolve by primary name
        FunctionDefinition def;
        String functionName;
        for (int i = 0; i < 10; i++) {
            functionName = randomCapitalizedString("dummyfunction");
            def = r.resolveFunction(r.resolveAlias(functionName));
            assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());
        }

        // Resolve by alias
        for (int i = 0; i < 10; i++) {
            functionName = randomCapitalizedString("dummyfunc");
            def = r.resolveFunction(r.resolveAlias(functionName));
            assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());
        }

        // Not resolved
        QlIllegalArgumentException e = expectThrows(
            QlIllegalArgumentException.class,
            () -> r.resolveFunction(r.resolveAlias("Dummy_Function"))
        );
        assertThat(e.getMessage(), is("Cannot find function dummy_function; this should have been caught during analysis"));

        e = expectThrows(QlIllegalArgumentException.class, () -> r.resolveFunction(r.resolveAlias("dummy_Function")));
        assertThat(e.getMessage(), is("Cannot find function dummy_function; this should have been caught during analysis"));
    }

    public void testUnaryFunction() {
        UnresolvedFunction ur = uf(DEFAULT, mock(Expression.class));
        FunctionRegistry r = new EsqlFunctionRegistry(defineDummyUnaryFunction(ur));
        FunctionDefinition def = r.resolveFunction(ur.name());

        // No children aren't supported
        ParsingException e = expectThrows(ParsingException.class, () -> uf(DEFAULT).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(
            ParsingException.class,
            () -> uf(DEFAULT, mock(Expression.class), mock(Expression.class)).buildResolved(randomConfiguration(), def)
        );
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));
    }

    private static UnresolvedFunction uf(FunctionResolutionStrategy resolutionStrategy, Expression... children) {
        return new UnresolvedFunction(SourceTests.randomSource(), "dummyFunction", resolutionStrategy, Arrays.asList(children));
    }

    private static FunctionDefinition defineDummyFunction(UnresolvedFunction ur, String... names) {
        return def(FunctionRegistryTests.DummyFunction.class, (Source l, Expression e) -> {
            assertSame(e, ur.children().get(0));
            return new FunctionRegistryTests.DummyFunction(l);
        }, names);
    }

    private static FunctionDefinition defineDummyUnaryFunction(UnresolvedFunction ur) {
        return defineDummyFunction(ur, "dummyFunction");
    }

    private String randomCapitalizedString(String input) {
        StringBuilder output = new StringBuilder();
        for (char c : input.toCharArray()) {
            if (randomBoolean()) {
                output.append(Character.toUpperCase(c));
            } else {
                output.append(c);
            }
        }
        return output.toString();
    }
}
