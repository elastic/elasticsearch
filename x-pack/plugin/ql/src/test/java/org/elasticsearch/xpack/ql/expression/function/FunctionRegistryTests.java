/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.tree.SourceTests;
import org.elasticsearch.xpack.ql.type.DataType;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.ql.TestUtils.randomConfiguration;
import static org.elasticsearch.xpack.ql.expression.function.FunctionRegistry.def;
import static org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction.ResolutionType.DISTINCT;
import static org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction.ResolutionType.EXTRACT;
import static org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction.ResolutionType.STANDARD;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class FunctionRegistryTests extends ESTestCase {

    public void testNoArgFunction() {
        UnresolvedFunction ur = uf(STANDARD);
        FunctionRegistry r = new FunctionRegistry(def(DummyFunction.class, DummyFunction::new, "DUMMY_FUNCTION"));
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
                uf(DISTINCT).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("does not support DISTINCT yet it was specified"));

        // Any children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD, mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects no arguments"));
    }

    public void testUnaryFunction() {
        UnresolvedFunction ur = uf(STANDARD, mock(Expression.class));
        FunctionRegistry r = new FunctionRegistry(def(DummyFunction.class, (Source l, Expression e) -> {
            assertSame(e, ur.children().get(0));
            return new DummyFunction(l);
        }, "DUMMY_FUNCTION"));
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertFalse(def.extractViable());
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
                uf(DISTINCT, mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("does not support DISTINCT yet it was specified"));

        // No children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
            uf(STANDARD, mock(Expression.class), mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));
    }

    public void testUnaryDistinctAwareFunction() {
        boolean urIsDistinct = randomBoolean();
        UnresolvedFunction ur = uf(urIsDistinct ? DISTINCT : STANDARD, mock(Expression.class));
        FunctionRegistry r = new FunctionRegistry(def(DummyFunction.class, (Source l, Expression e, boolean distinct) -> {
                    assertEquals(urIsDistinct, distinct);
                    assertSame(e, ur.children().get(0));
                    return new DummyFunction(l);
        }, "DUMMY_FUNCTION"));
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());
        assertFalse(def.extractViable());

        // No children aren't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
                uf(STANDARD).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD, mock(Expression.class), mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));
    }

    public void testDateTimeFunction() {
        boolean urIsExtract = randomBoolean();
        UnresolvedFunction ur = uf(urIsExtract ? EXTRACT : STANDARD, mock(Expression.class));
        ZoneId providedTimeZone = randomZone().normalized();
        Configuration providedConfiguration = randomConfiguration(providedTimeZone);
        FunctionRegistry r = new FunctionRegistry(def(DummyFunction.class, (Source l, Expression e, ZoneId zi) -> {
                    assertEquals(providedTimeZone, zi);
                    assertSame(e, ur.children().get(0));
                    return new DummyFunction(l);
        }, "DUMMY_FUNCTION"));
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.source(), ur.buildResolved(providedConfiguration, def).source());
        assertTrue(def.extractViable());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
                uf(DISTINCT, mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("does not support DISTINCT yet it was specified"));

        // No children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD, mock(Expression.class), mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly one argument"));
    }

    public void testBinaryFunction() {
        UnresolvedFunction ur = uf(STANDARD, mock(Expression.class), mock(Expression.class));
        FunctionRegistry r = new FunctionRegistry(def(DummyFunction.class, (Source l, Expression lhs, Expression rhs) -> {
                    assertSame(lhs, ur.children().get(0));
                    assertSame(rhs, ur.children().get(1));
                    return new DummyFunction(l);
        }, "DUMMY_FUNCTION"));
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());
        assertFalse(def.extractViable());

        // Distinct isn't supported
        ParsingException e = expectThrows(ParsingException.class, () ->
                uf(DISTINCT, mock(Expression.class), mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("does not support DISTINCT yet it was specified"));

        // No children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly two arguments"));

        // One child isn't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD, mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly two arguments"));

        // Many children aren't supported
        e = expectThrows(ParsingException.class, () ->
                uf(STANDARD, mock(Expression.class), mock(Expression.class), mock(Expression.class))
                    .buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly two arguments"));
    }
    
    public void testAliasNameIsTheSameAsAFunctionName() {
        FunctionRegistry r = new FunctionRegistry(def(DummyFunction.class, DummyFunction::new, "DUMMY_FUNCTION", "ALIAS"));
        QlIllegalArgumentException iae = expectThrows(QlIllegalArgumentException.class, () ->
                r.register(def(DummyFunction2.class, DummyFunction2::new, "DUMMY_FUNCTION2", "DUMMY_FUNCTION")));
        assertEquals("alias [DUMMY_FUNCTION] is used by [DUMMY_FUNCTION] and [DUMMY_FUNCTION2]", iae.getMessage());
    }
    
    public void testDuplicateAliasInTwoDifferentFunctionsFromTheSameBatch() {
        QlIllegalArgumentException iae = expectThrows(QlIllegalArgumentException.class, () ->
                new FunctionRegistry(def(DummyFunction.class, DummyFunction::new, "DUMMY_FUNCTION", "ALIAS"),
                        def(DummyFunction2.class, DummyFunction2::new, "DUMMY_FUNCTION2", "ALIAS")));
        assertEquals("alias [ALIAS] is used by [DUMMY_FUNCTION(ALIAS)] and [DUMMY_FUNCTION2]", iae.getMessage());
    }
    
    public void testDuplicateAliasInTwoDifferentFunctionsFromTwoDifferentBatches() {
        FunctionRegistry r = new FunctionRegistry(def(DummyFunction.class, DummyFunction::new, "DUMMY_FUNCTION", "ALIAS"));
        QlIllegalArgumentException iae = expectThrows(QlIllegalArgumentException.class, () ->
                r.register(def(DummyFunction2.class, DummyFunction2::new, "DUMMY_FUNCTION2", "ALIAS")));
        assertEquals("alias [ALIAS] is used by [DUMMY_FUNCTION] and [DUMMY_FUNCTION2]", iae.getMessage());
    }

    public void testFunctionResolving() {
        UnresolvedFunction ur = uf(STANDARD, mock(Expression.class));
        FunctionRegistry r = new FunctionRegistry(def(DummyFunction.class, (Source l, Expression e) -> {
            assertSame(e, ur.children().get(0));
            return new DummyFunction(l);
        }, "DUMMY_FUNCTION", "DUMMY_FUNC"));

        // Resolve by primary name
        FunctionDefinition def = r.resolveFunction(r.resolveAlias("DuMMy_FuncTIon"));
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());

        def = r.resolveFunction(r.resolveAlias("Dummy_Function"));
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());

        def = r.resolveFunction(r.resolveAlias("dummy_function"));
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());

        def = r.resolveFunction(r.resolveAlias("DUMMY_FUNCTION"));
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());

        // Resolve by alias
        def = r.resolveFunction(r.resolveAlias("DumMy_FunC"));
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());

        def = r.resolveFunction(r.resolveAlias("dummy_func"));
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());

        def = r.resolveFunction(r.resolveAlias("DUMMY_FUNC"));
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());

        // Not resolved
        QlIllegalArgumentException e = expectThrows(QlIllegalArgumentException.class,
            () -> r.resolveFunction(r.resolveAlias("DummyFunction")));
        assertThat(e.getMessage(),
            is("Cannot find function DUMMYFUNCTION; this should have been caught during analysis"));

        e = expectThrows(QlIllegalArgumentException.class,
            () -> r.resolveFunction(r.resolveAlias("dummyFunction")));
        assertThat(e.getMessage(),
            is("Cannot find function DUMMYFUNCTION; this should have been caught during analysis"));
    }

    private UnresolvedFunction uf(UnresolvedFunction.ResolutionType resolutionType, Expression... children) {
        return new UnresolvedFunction(SourceTests.randomSource(), "DUMMY_FUNCTION", resolutionType, Arrays.asList(children));
    }
    
    public static class DummyFunction extends ScalarFunction {
        public DummyFunction(Source source) {
            super(source, emptyList());
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
    
    public static class DummyFunction2 extends DummyFunction {
        public DummyFunction2(Source source) {
            super(source);
        }
    }
}
