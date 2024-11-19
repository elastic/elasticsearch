/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.ParsingException;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry.def;
import static org.elasticsearch.xpack.esql.expression.function.FunctionResolutionStrategy.DEFAULT;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class EsqlFunctionRegistryTests extends ESTestCase {

    public void testNoArgFunction() {
        UnresolvedFunction ur = uf(DEFAULT);
        EsqlFunctionRegistry r = new EsqlFunctionRegistry(def(DummyFunction.class, DummyFunction::new, "dummyFunction"));
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());
    }

    public void testBinaryFunction() {
        UnresolvedFunction ur = uf(DEFAULT, mock(Expression.class), mock(Expression.class));
        EsqlFunctionRegistry r = new EsqlFunctionRegistry(def(DummyFunction.class, (Source l, Expression lhs, Expression rhs) -> {
            assertSame(lhs, ur.children().get(0));
            assertSame(rhs, ur.children().get(1));
            return new DummyFunction(l);
        }, "dummyFunction"));
        FunctionDefinition def = r.resolveFunction(ur.name());
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());

        // No children aren't supported
        ParsingException e = expectThrows(ParsingException.class, () -> uf(DEFAULT).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly two arguments"));

        // One child isn't supported
        e = expectThrows(ParsingException.class, () -> uf(DEFAULT, mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), endsWith("expects exactly two arguments"));

        // Many children aren't supported
        e = expectThrows(
            ParsingException.class,
            () -> uf(DEFAULT, mock(Expression.class), mock(Expression.class), mock(Expression.class)).buildResolved(
                randomConfiguration(),
                def
            )
        );
        assertThat(e.getMessage(), endsWith("expects exactly two arguments"));
    }

    public void testAliasNameIsTheSameAsAFunctionName() {
        EsqlFunctionRegistry r = new EsqlFunctionRegistry(def(DummyFunction.class, DummyFunction::new, "DUMMY_FUNCTION", "ALIAS"));
        QlIllegalArgumentException iae = expectThrows(
            QlIllegalArgumentException.class,
            () -> r.register(def(DummyFunction2.class, DummyFunction2::new, "DUMMY_FUNCTION2", "DUMMY_FUNCTION"))
        );
        assertEquals("alias [DUMMY_FUNCTION] is used by [DUMMY_FUNCTION] and [DUMMY_FUNCTION2]", iae.getMessage());
    }

    public void testDuplicateAliasInTwoDifferentFunctionsFromTheSameBatch() {
        QlIllegalArgumentException iae = expectThrows(
            QlIllegalArgumentException.class,
            () -> new EsqlFunctionRegistry(
                def(DummyFunction.class, DummyFunction::new, "DUMMY_FUNCTION", "ALIAS"),
                def(DummyFunction2.class, DummyFunction2::new, "DUMMY_FUNCTION2", "ALIAS")
            )
        );
        assertEquals("alias [ALIAS] is used by [DUMMY_FUNCTION(ALIAS)] and [DUMMY_FUNCTION2]", iae.getMessage());
    }

    public void testDuplicateAliasInTwoDifferentFunctionsFromTwoDifferentBatches() {
        EsqlFunctionRegistry r = new EsqlFunctionRegistry(def(DummyFunction.class, DummyFunction::new, "DUMMY_FUNCTION", "ALIAS"));
        QlIllegalArgumentException iae = expectThrows(
            QlIllegalArgumentException.class,
            () -> r.register(def(DummyFunction2.class, DummyFunction2::new, "DUMMY_FUNCTION2", "ALIAS"))
        );
        assertEquals("alias [ALIAS] is used by [DUMMY_FUNCTION] and [DUMMY_FUNCTION2]", iae.getMessage());
    }

    public void testFunctionResolving() {
        UnresolvedFunction ur = uf(DEFAULT, mock(Expression.class));
        EsqlFunctionRegistry r = new EsqlFunctionRegistry(defineDummyFunction(ur, "dummyfunction", "dummyfunc"));

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
        EsqlFunctionRegistry r = new EsqlFunctionRegistry(defineDummyUnaryFunction(ur));
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

    public void testConfigurationOptionalFunction() {
        UnresolvedFunction ur = uf(DEFAULT, mock(Expression.class));
        FunctionDefinition def;
        EsqlFunctionRegistry r = new EsqlFunctionRegistry(
            def(DummyConfigurationOptionalArgumentFunction.class, (Source l, Expression e, Configuration c) -> {
                assertSame(e, ur.children().get(0));
                return new DummyConfigurationOptionalArgumentFunction(l, List.of(ur), c);
            }, "dummy")
        );
        def = r.resolveFunction(r.resolveAlias("DUMMY"));
        assertEquals(ur.source(), ur.buildResolved(randomConfiguration(), def).source());
    }

    private static UnresolvedFunction uf(FunctionResolutionStrategy resolutionStrategy, Expression... children) {
        return new UnresolvedFunction(SourceTests.randomSource(), "dummyFunction", resolutionStrategy, Arrays.asList(children));
    }

    private static FunctionDefinition defineDummyFunction(UnresolvedFunction ur, String... names) {
        return def(DummyFunction.class, (Source l, Expression e) -> {
            assertSame(e, ur.children().get(0));
            return new DummyFunction(l);
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

    public static class DummyFunction extends ScalarFunction {
        public DummyFunction(Source source) {
            super(source, emptyList());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
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
    }

    public static class DummyFunction2 extends DummyFunction {
        public DummyFunction2(Source source) {
            super(source);
        }
    }

    public static class DummyConfigurationOptionalArgumentFunction extends EsqlConfigurationFunction implements OptionalArgument {

        public DummyConfigurationOptionalArgumentFunction(Source source, List<Expression> fields, Configuration configuration) {
            super(source, fields, configuration);
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataType dataType() {
            return null;
        }

        @Override
        public Expression replaceChildren(List<Expression> newChildren) {
            return new DummyConfigurationOptionalArgumentFunction(source(), newChildren, configuration());
        }

        @Override
        protected NodeInfo<? extends Expression> info() {
            return NodeInfo.create(this, DummyConfigurationOptionalArgumentFunction::new, children(), configuration());
        }

        @Override
        public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
            return null;
        }
    }
}
