/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.LocationTests;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.sql.expression.function.FunctionRegistry.def;
import static org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction.ResolutionType.DISTINCT;
import static org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction.ResolutionType.EXTRACT;
import static org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction.ResolutionType.STANDARD;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class FunctionRegistryTests extends ESTestCase {
    public void testNoArgFunction() {
        UnresolvedFunction ur = uf(STANDARD);
        FunctionRegistry r = new FunctionRegistry(Collections.singletonList(def(DummyFunction.class, DummyFunction::new)));
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.location(), ur.buildResolved(randomTimeZone(), def).location());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
                uf(DISTINCT).buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("does not support DISTINCT yet it was specified"));

        // Any children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD, mock(Expression.class)).buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("expects no arguments"));
    }

    public void testUnaryFunction() {
        UnresolvedFunction ur = uf(STANDARD, mock(Expression.class));
        FunctionRegistry r = new FunctionRegistry(Collections.singletonList(
            def(DummyFunction.class, (Location l, Expression e) -> {
            assertSame(e, ur.children().get(0));
            return new DummyFunction(l);
        })));
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertFalse(def.datetime());
        assertEquals(ur.location(), ur.buildResolved(randomTimeZone(), def).location());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
                uf(DISTINCT, mock(Expression.class)).buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("does not support DISTINCT yet it was specified"));

        // No children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD).buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
            uf(STANDARD, mock(Expression.class), mock(Expression.class)).buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));
    }

    public void testUnaryDistinctAwareFunction() {
        boolean urIsDistinct = randomBoolean();
        UnresolvedFunction ur = uf(urIsDistinct ? DISTINCT : STANDARD, mock(Expression.class));
        FunctionRegistry r = new FunctionRegistry(Collections.singletonList(
            def(DummyFunction.class, (Location l, Expression e, boolean distinct) -> {
                assertEquals(urIsDistinct, distinct);
                assertSame(e, ur.children().get(0));
                return new DummyFunction(l);
            })));
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.location(), ur.buildResolved(randomTimeZone(), def).location());
        assertFalse(def.datetime());

        // No children aren't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
                uf(STANDARD).buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD, mock(Expression.class), mock(Expression.class)).buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));
    }

    public void testDateTimeFunction() {
        boolean urIsExtract = randomBoolean();
        UnresolvedFunction ur = uf(urIsExtract ? EXTRACT : STANDARD, mock(Expression.class));
        TimeZone providedTimeZone = randomTimeZone();
        FunctionRegistry r = new FunctionRegistry(Collections.singletonList(
            def(DummyFunction.class, (Location l, Expression e, TimeZone tz) -> {
                assertEquals(providedTimeZone, tz);
                assertSame(e, ur.children().get(0));
                return new DummyFunction(l);
            })));
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.location(), ur.buildResolved(providedTimeZone, def).location());
        assertTrue(def.datetime());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
                uf(DISTINCT, mock(Expression.class)).buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("does not support DISTINCT yet it was specified"));

        // No children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD).buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD, mock(Expression.class), mock(Expression.class)).buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));
    }

    public void testBinaryFunction() {
        UnresolvedFunction ur = uf(STANDARD, mock(Expression.class), mock(Expression.class));
        FunctionRegistry r = new FunctionRegistry(Collections.singletonList(
            def(DummyFunction.class, (Location l, Expression lhs, Expression rhs) -> {
                assertSame(lhs, ur.children().get(0));
                assertSame(rhs, ur.children().get(1));
                return new DummyFunction(l);
            })));
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.location(), ur.buildResolved(randomTimeZone(), def).location());
        assertFalse(def.datetime());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
                uf(DISTINCT, mock(Expression.class), mock(Expression.class)).buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("does not support DISTINCT yet it was specified"));

        // No children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD).buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("expects exactly two arguments"));

        // One child isn't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD, mock(Expression.class)).buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("expects exactly two arguments"));

        // Many children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD, mock(Expression.class), mock(Expression.class), mock(Expression.class))
                    .buildResolved(randomTimeZone(), def));
        assertThat(e.getMessage(), endsWith("expects exactly two arguments"));
    }

    public void testFunctionResolving() {
        UnresolvedFunction ur = uf(STANDARD, mock(Expression.class));
        FunctionRegistry r = new FunctionRegistry(
            Collections.singletonList(def(DummyFunction.class, (Location l, Expression e) -> {
            assertSame(e, ur.children().get(0));
            return new DummyFunction(l);
        }, "DUMMY_FUNC")));

        // Resolve by primary name
        FunctionDefinition def = r.resolveFunction(r.resolveAlias("DuMMy_FuncTIon"));
        assertEquals(ur.location(), ur.buildResolved(randomTimeZone(), def).location());

        def = r.resolveFunction(r.resolveAlias("Dummy_Function"));
        assertEquals(ur.location(), ur.buildResolved(randomTimeZone(), def).location());

        def = r.resolveFunction(r.resolveAlias("dummy_function"));
        assertEquals(ur.location(), ur.buildResolved(randomTimeZone(), def).location());

        def = r.resolveFunction(r.resolveAlias("DUMMY_FUNCTION"));
        assertEquals(ur.location(), ur.buildResolved(randomTimeZone(), def).location());

        // Resolve by alias
        def = r.resolveFunction(r.resolveAlias("DumMy_FunC"));
        assertEquals(ur.location(), ur.buildResolved(randomTimeZone(), def).location());

        def = r.resolveFunction(r.resolveAlias("dummy_func"));
        assertEquals(ur.location(), ur.buildResolved(randomTimeZone(), def).location());

        def = r.resolveFunction(r.resolveAlias("DUMMY_FUNC"));
        assertEquals(ur.location(), ur.buildResolved(randomTimeZone(), def).location());

        // Not resolved
        SqlIllegalArgumentException e = expectThrows(SqlIllegalArgumentException.class,
            () -> r.resolveFunction(r.resolveAlias("DummyFunction")));
        assertThat(e.getMessage(),
            is("Cannot find function DUMMYFUNCTION; this should have been caught during analysis"));

        e = expectThrows(SqlIllegalArgumentException.class,
            () -> r.resolveFunction(r.resolveAlias("dummyFunction")));
        assertThat(e.getMessage(),
            is("Cannot find function DUMMYFUNCTION; this should have been caught during analysis"));
    }

    private UnresolvedFunction uf(UnresolvedFunction.ResolutionType resolutionType, Expression... children) {
        return new UnresolvedFunction(LocationTests.randomLocation(), "DUMMY_FUNCTION", resolutionType, Arrays.asList(children));
    }

    public static class DummyFunction extends ScalarFunction {
        public DummyFunction(Location location) {
            super(location, emptyList());
        }

        @Override
        protected NodeInfo<DummyFunction> info() {
            return NodeInfo.create(this);
        }

        @Override
        public Expression replaceChildren(List<Expression> newChildren) {
            throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
        }

        @Override
        public DataType dataType() {
            return null;
        }

        @Override
        public ScriptTemplate asScript() {
            return null;
        }

        @Override
        protected Pipe makePipe() {
            return null;
        }
    }
}
