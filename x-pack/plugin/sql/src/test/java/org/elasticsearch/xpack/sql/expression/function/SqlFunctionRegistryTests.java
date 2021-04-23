/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistryTests.DummyFunction;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.TestUtils.randomConfiguration;
import static org.elasticsearch.xpack.ql.expression.function.FunctionRegistryTests.defineDummyNoArgFunction;
import static org.elasticsearch.xpack.ql.expression.function.FunctionRegistryTests.defineDummyUnaryFunction;
import static org.elasticsearch.xpack.ql.expression.function.FunctionRegistryTests.uf;
import static org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy.DEFAULT;
import static org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry.def;
import static org.elasticsearch.xpack.sql.expression.function.SqlFunctionResolution.DISTINCT;
import static org.elasticsearch.xpack.sql.expression.function.SqlFunctionResolution.EXTRACT;
import static org.hamcrest.Matchers.endsWith;
import static org.mockito.Mockito.mock;

public class SqlFunctionRegistryTests extends ESTestCase {

    public void testNoArgFunction() {
        UnresolvedFunction ur = uf(DEFAULT);
        FunctionRegistry r = new SqlFunctionRegistry(defineDummyNoArgFunction());
        FunctionDefinition def = r.resolveFunction(ur.name());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
            uf(DISTINCT).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("Cannot use DISTINCT on non-SQL function DUMMY_FUNCTION()"));

        // Any children aren't supported
        e = expectThrows(ParsingException.class, () ->
            uf(DEFAULT, mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects no arguments"));
    }

    public void testUnaryFunction() {
        UnresolvedFunction ur = uf(DEFAULT, mock(Expression.class));
        FunctionRegistry r = new SqlFunctionRegistry(defineDummyUnaryFunction(ur));
        FunctionDefinition def = r.resolveFunction(ur.name());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
            uf(DISTINCT, mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("Cannot use DISTINCT on non-SQL function DUMMY_FUNCTION()"));

        // No children aren't supported
        e = expectThrows(ParsingException.class, () ->
            uf(DEFAULT).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
            uf(DEFAULT, mock(Expression.class), mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));
    }

    public void testUnaryDistinctAwareFunction() {
        boolean urIsDistinct = randomBoolean();
        UnresolvedFunction ur = uf(urIsDistinct ? DISTINCT : DEFAULT, mock(Expression.class));
        FunctionDefinition definition = def(DummyFunction.class, (Source l, Expression e, Boolean distinct) -> {
            assertEquals(urIsDistinct, distinct);
            assertSame(e, ur.children().get(0));
            return new DummyFunction(l);
        }, "DUMMY_FUNCTION");
        FunctionRegistry r = new SqlFunctionRegistry(definition);
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());

        assertEquals(SqlFunctionDefinition.class, def.getClass());
        assertFalse(((SqlFunctionDefinition) def).extractViable());

        // No children aren't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
            uf(DEFAULT).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
            uf(DEFAULT, mock(Expression.class), mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));
    }

    public void testDateTimeFunction() {
        boolean urIsExtract = randomBoolean();
        Expression exprMock = mock(Expression.class);
        UnresolvedFunction ur = uf(urIsExtract ? EXTRACT : DEFAULT, exprMock);
        ZoneId providedTimeZone = randomZone().normalized();
        Configuration providedConfiguration = randomConfiguration(providedTimeZone);
        FunctionRegistry r = new SqlFunctionRegistry(def(DummyFunction.class, (Source l, Expression e, ZoneId zi) -> {
            assertEquals(providedTimeZone, zi);
            assertSame(e, ur.children().get(0));
            return new DummyFunction(l);
        }, "DUMMY_FUNCTION"));
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.source(), ur.buildResolved(providedConfiguration, def).source());
        assertEquals(SqlFunctionDefinition.class, def.getClass());
        assertTrue(((SqlFunctionDefinition) def).extractViable());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
            uf(DISTINCT, exprMock).buildResolved(providedConfiguration, def));
        assertThat(e.getMessage(), endsWith("does not support DISTINCT yet it was specified"));

        // No children aren't supported
        e = expectThrows(ParsingException.class, () ->
            uf(DEFAULT).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
            uf(DEFAULT, exprMock, exprMock).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));
    }
}

