/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.LocationTests;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import static org.elasticsearch.xpack.sql.expression.function.FunctionRegistry.def;
import static org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction.ResolutionType.DISTINCT;
import static org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction.ResolutionType.EXTRACT;
import static org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction.ResolutionType.STANDARD;
import static org.hamcrest.Matchers.endsWith;
import static org.mockito.Mockito.mock;
import static java.util.Collections.emptyList;

public class FunctionRegistryTests extends ESTestCase {
    public void testNoArgFunction() {
        UnresolvedFunction ur = uf(STANDARD);
        FunctionRegistry r = new FunctionRegistry(Arrays.asList(def(Dummy.class, Dummy::new)));
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
        FunctionRegistry r = new FunctionRegistry(Arrays.asList(def(Dummy.class, (Location l, Expression e) -> {
            assertSame(e, ur.children().get(0));
            return new Dummy(l);
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
        FunctionRegistry r = new FunctionRegistry(Arrays.asList(def(Dummy.class, (Location l, Expression e, boolean distinct) -> {
            assertEquals(urIsDistinct, distinct);
            assertSame(e, ur.children().get(0));
            return new Dummy(l);
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
        FunctionRegistry r = new FunctionRegistry(Arrays.asList(def(Dummy.class, (Location l, Expression e, TimeZone tz) -> {
            assertEquals(providedTimeZone, tz);
            assertSame(e, ur.children().get(0));
            return new Dummy(l);
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
        FunctionRegistry r = new FunctionRegistry(Arrays.asList(def(Dummy.class, (Location l, Expression lhs, Expression rhs) -> {
            assertSame(lhs, ur.children().get(0));
            assertSame(rhs, ur.children().get(1));
            return new Dummy(l);
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

    private UnresolvedFunction uf(UnresolvedFunction.ResolutionType resolutionType, Expression... children) {
        return new UnresolvedFunction(LocationTests.randomLocation(), "dummy", resolutionType, Arrays.asList(children));
    }

    public static class Dummy extends ScalarFunction {
        public Dummy(Location location) {
            super(location, emptyList());
        }

        @Override
        protected NodeInfo<Dummy> info() {
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
