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
import org.joda.time.DateTimeZone;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.sql.expression.function.FunctionRegistry.def;
import static org.hamcrest.Matchers.endsWith;
import static org.mockito.Mockito.mock;
import static java.util.Collections.emptyList;

public class FunctionRegistryTests extends ESTestCase {
    public void testNoArgFunction() {
        UnresolvedFunction ur = uf(false);
        FunctionRegistry r = new FunctionRegistry(Arrays.asList(def(Dummy.class, Dummy::new)));
        assertEquals(ur.location(), r.resolveFunction(ur, randomDateTimeZone()).location());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(true), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("does not support DISTINCT yet it was specified"));

        // Any children aren't supported
        e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(false, mock(Expression.class)), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("expects no arguments"));
    }

    public void testUnaryFunction() {
        UnresolvedFunction ur = uf(false, mock(Expression.class));
        FunctionRegistry r = new FunctionRegistry(Arrays.asList(def(Dummy.class, (Location l, Expression e) -> {
            assertSame(e, ur.children().get(0));
            return new Dummy(l);
        })));
        assertEquals(ur.location(), r.resolveFunction(ur, randomDateTimeZone()).location());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(true, mock(Expression.class)), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("does not support DISTINCT yet it was specified"));

        // No children aren't supported
        e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(false), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(false, mock(Expression.class), mock(Expression.class)), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));
    }

    public void testUnaryDistinctAwareFunction() {
        UnresolvedFunction ur = uf(randomBoolean(), mock(Expression.class));
        FunctionRegistry r = new FunctionRegistry(Arrays.asList(def(Dummy.class, (Location l, Expression e, boolean distinct) -> {
            assertEquals(ur.distinct(), distinct);
            assertSame(e, ur.children().get(0));
            return new Dummy(l);
        })));
        assertEquals(ur.location(), r.resolveFunction(ur, randomDateTimeZone()).location());

        // No children aren't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(false), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(false, mock(Expression.class), mock(Expression.class)), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));
    }

    public void testTimeZoneAwareFunction() {
        UnresolvedFunction ur = uf(false, mock(Expression.class));
        DateTimeZone providedTimeZone = randomDateTimeZone();
        FunctionRegistry r = new FunctionRegistry(Arrays.asList(def(Dummy.class, (Location l, Expression e, DateTimeZone tz) -> {
            assertEquals(providedTimeZone, tz);
            assertSame(e, ur.children().get(0));
            return new Dummy(l);
        })));
        assertEquals(ur.location(), r.resolveFunction(ur, providedTimeZone).location());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(true, mock(Expression.class)), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("does not support DISTINCT yet it was specified"));

        // No children aren't supported
        e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(false), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(false, mock(Expression.class), mock(Expression.class)), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));
    }

    public void testBinaryFunction() {
        UnresolvedFunction ur = uf(false, mock(Expression.class), mock(Expression.class));
        FunctionRegistry r = new FunctionRegistry(Arrays.asList(def(Dummy.class, (Location l, Expression lhs, Expression rhs) -> {
            assertSame(lhs, ur.children().get(0));
            assertSame(rhs, ur.children().get(1));
            return new Dummy(l);
        })));
        assertEquals(ur.location(), r.resolveFunction(ur, randomDateTimeZone()).location());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(true, mock(Expression.class), mock(Expression.class)), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("does not support DISTINCT yet it was specified"));

        // No children aren't supported
        e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(false), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("expects exactly two arguments"));

        // One child isn't supported
        e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(false, mock(Expression.class)), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("expects exactly two arguments"));

        // Many children aren't supported
        e = expectThrows(ParsingException.class, () ->
            r.resolveFunction(uf(false, mock(Expression.class), mock(Expression.class), mock(Expression.class)), randomDateTimeZone()));
        assertThat(e.getMessage(), endsWith("expects exactly two arguments"));
    }

    private UnresolvedFunction uf(boolean distinct, Expression... children) {
        return new UnresolvedFunction(LocationTests.randomLocation(), "dummy", distinct, Arrays.asList(children));
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
        protected ProcessorDefinition makeProcessorDefinition() {
            return null;
        }
    }
}
