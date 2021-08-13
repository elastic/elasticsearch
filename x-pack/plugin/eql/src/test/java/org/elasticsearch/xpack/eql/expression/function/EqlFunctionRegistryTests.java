/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistryTests.DummyFunction;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.tree.Source;

import static org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry.def;
import static org.elasticsearch.xpack.ql.TestUtils.randomConfiguration;
import static org.elasticsearch.xpack.ql.expression.function.FunctionRegistryTests.uf;
import static org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy.DEFAULT;
import static org.hamcrest.Matchers.endsWith;
import static org.mockito.Mockito.mock;

public class EqlFunctionRegistryTests extends ESTestCase {

    public void testBinaryCaseAwareFunction() {
        boolean caseAware = randomBoolean();
        UnresolvedFunction ur = uf(caseAware ? EqlFunctionResolution.CASE_INSENSITIVE : DEFAULT,
            mock(Expression.class), mock(Expression.class));
        FunctionDefinition definition = def(DummyFunction.class, (Source l, Expression left, Expression right, boolean insensitive) -> {
            assertEquals(caseAware, insensitive);
            assertSame(left, ur.children().get(0));
            assertSame(right, ur.children().get(1));
            return new DummyFunction(l);
        }, "DUMMY_FUNCTION");
        FunctionRegistry r = new EqlFunctionRegistry(definition);
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());

        assertEquals(EqlFunctionDefinition.class, def.getClass());
        assertFalse(((EqlFunctionDefinition) def).isCaseAware());

        // No children aren't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
            uf(DEFAULT).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("error building [DUMMY_FUNCTION]: expects exactly two arguments"));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
            uf(DEFAULT, mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly two arguments"));
    }

    public void testTernaryCaseAwareWithOptionalFunction() {
        boolean caseAware = randomBoolean();
        boolean hasOptional = randomBoolean();
        UnresolvedFunction ur = uf(caseAware ? EqlFunctionResolution.CASE_INSENSITIVE : DEFAULT,
            mock(Expression.class), mock(Expression.class), mock(Expression.class));
        FunctionDefinition definition = def(DummyFunction.class,
            (Source l, Expression one, Expression two, Expression three, boolean insensitive) -> {
                assertEquals(caseAware, insensitive);
                assertSame(one, ur.children().get(0));
                assertSame(two, ur.children().get(1));
                assertSame(three, ur.children().get(2));
                return new DummyFunction(l);
            }, "DUMMY_FUNCTION");
        FunctionRegistry r = new EqlFunctionRegistry(definition);
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());

        assertEquals(EqlFunctionDefinition.class, def.getClass());
        assertFalse(((EqlFunctionDefinition) def).isCaseAware());

        String message = "expects exactly three arguments";
        // No children aren't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
            uf(DEFAULT).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith(message));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
            uf(DEFAULT, mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith(message));
    }
}

