/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.OnlySurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FoldablesConvertFunction;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.constructorWithFunctionInfo;
import static org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry.def;
import static org.elasticsearch.xpack.esql.expression.function.FunctionResolutionStrategy.DEFAULT;
import static org.hamcrest.Matchers.containsString;
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
        assertThat(e.getMessage(), containsString("expects exactly two arguments"));

        // One child isn't supported
        e = expectThrows(ParsingException.class, () -> uf(DEFAULT, mock(Expression.class)).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), containsString("expects exactly two arguments"));

        // Many children aren't supported
        e = expectThrows(
            ParsingException.class,
            () -> uf(DEFAULT, mock(Expression.class), mock(Expression.class), mock(Expression.class)).buildResolved(
                randomConfiguration(),
                def
            )
        );
        assertThat(e.getMessage(), containsString("expects exactly two arguments"));
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
        assertThat(e.getMessage(), containsString("expects exactly one argument"));

        // Multiple children aren't supported
        e = expectThrows(
            ParsingException.class,
            () -> uf(DEFAULT, mock(Expression.class), mock(Expression.class)).buildResolved(randomConfiguration(), def)
        );
        assertThat(e.getMessage(), containsString("expects exactly one argument"));
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

        ParsingException e = expectThrows(ParsingException.class, () -> uf(DEFAULT).buildResolved(randomConfiguration(), def));
        assertThat(e.getMessage(), containsString("expects exactly one argument"));
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

    public void testRegisteredFunctionHaveTests() {
        EsqlFunctionRegistry registry = new EsqlFunctionRegistry().snapshotRegistry();
        Set<String> errors = new TreeSet<>();
        for (FunctionDefinition def : registry.listFunctions()) {
            checkFunctionTestExists(errors, def, "Tests", AbstractFunctionTestCase.class);
            if (takesParameters(def)) {
                checkFunctionTestExists(errors, def, "ErrorTests", ErrorsForCasesWithoutExamplesTestCase.class);
            }
            boolean isSerializable = false == OnlySurrogateExpression.class.isAssignableFrom(def.clazz())
                && false == FoldablesConvertFunction.class.isAssignableFrom(def.clazz())
                && false == InferenceFunction.class.isAssignableFrom(def.clazz());
            if (isSerializable) {
                checkFunctionTestExists(errors, def, "SerializationTests", AbstractExpressionSerializationTests.class);
            }
        }
        assertMap(
            "function test errors",
            List.copyOf(errors),
            /*
             * We should add these missing tests when we can. It's *fine* to add to this list
             * while working on something, but released stuff should not have an entry here.
             */
            matchesList().item("org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTimeErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.AvgOverTimeErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinctErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinctOverTimeErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTimeErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.DeltaErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.DerivErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.FirstOverTimeErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.IdeltaErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.IncreaseErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.IrateErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTimeErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviationErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.MedianErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.PercentileOverTimeErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.PresentOverTimeErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.RateErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroidErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialExtentErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.StdDevErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.StddevOverTimeErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.SumErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.SumOverTimeErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.VarianceErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.VarianceOverTimeErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.aggregate.WeightedAvgErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhraseErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.fulltext.MultiMatchErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.fulltext.ScoreErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.grouping.BucketErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.grouping.TBucketErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.ClampErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.ClampTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMaxErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMinErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.conditional.GreatestErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.conditional.LeastErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToAggregateMetricDoubleErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateRangeErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateRangeTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDenseVectorErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeohexErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToVersionErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersectionErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSortErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvUnionErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.nulls.CoalesceErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.score.DecayErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StNPointsErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.string.ChunkErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.string.ContainsErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.string.TopSnippetsErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.util.DelayErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.scalar.util.DelayTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.vector.CosineSimilarityErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.vector.DotProductErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.vector.HammingErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.vector.KnnErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.vector.L1NormErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.vector.L2NormErrorTests is missing")
                .item("org.elasticsearch.xpack.esql.expression.function.vector.MagnitudeErrorTests is missing")
        );
    }

    private static void checkFunctionTestExists(
        Collection<String> errors,
        FunctionDefinition def,
        String suffix,
        Class<? extends ESTestCase> requiredSuperclass
    ) {
        Class<?> functionClass = def.clazz();
        String testClassName = functionClass.getName() + suffix;
        try {
            Class<?> testClass = Class.forName(testClassName);
            if (requiredSuperclass.isAssignableFrom(testClass) == false) {
                errors.add(testClassName + " doesn't extend " + requiredSuperclass.getSimpleName());
            }
        } catch (ClassNotFoundException e) {
            errors.add(testClassName + " is missing");
        }
    }

    /**
     * Does the function take any parameters?
     */
    private static boolean takesParameters(FunctionDefinition def) {
        for (Parameter p : constructorWithFunctionInfo(def.clazz()).getParameters()) {
            if (Source.class.isAssignableFrom(p.getType())) {
                continue;
            }
            if (Configuration.class.isAssignableFrom(p.getType())) {
                continue;
            }
            if (Expression.class.isAssignableFrom(p.getType())) {
                return true;
            }
            throw new IllegalStateException("unknown argument type " + p);
        }
        return false;
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
        public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
            return null;
        }
    }
}
