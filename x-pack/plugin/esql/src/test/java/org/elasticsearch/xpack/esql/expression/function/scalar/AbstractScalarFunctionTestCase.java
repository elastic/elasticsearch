/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.planner.EvalMapper;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.hamcrest.Matcher;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Base class for function tests.
 */
public abstract class AbstractScalarFunctionTestCase extends ESTestCase {
    /**
     * Generate a random value of the appropriate type to fit into blocks of {@code e}.
     */
    public static Literal randomLiteral(DataType type) {
        return new Literal(Source.EMPTY, switch (type.typeName()) {
            case "boolean" -> randomBoolean();
            case "byte" -> randomByte();
            case "short" -> randomShort();
            case "integer" -> randomInt();
            case "long" -> randomLong();
            case "date_period" -> Period.ofDays(randomInt(10));
            case "datetime" -> randomMillisUpToYear9999();
            case "double" -> randomDouble();
            case "float" -> randomFloat();
            case "keyword" -> randomAlphaOfLength(5);
            case "time_duration" -> Duration.ofMillis(randomNonNegativeLong());
            case "null" -> null;
            default -> throw new IllegalArgumentException("can't make random values for [" + type.typeName() + "]");
        }, type);
    }

    protected abstract List<Object> simpleData();

    protected abstract Expression expressionForSimpleData();

    protected abstract DataType expressionForSimpleDataType();

    protected abstract Matcher<Object> resultMatcher(List<Object> data);

    protected abstract String expectedEvaluatorSimpleToString();

    protected abstract Expression constantFoldable(List<Object> data);

    protected abstract List<ArgumentSpec> argSpec();

    protected final ArgumentSpec required(DataType... validTypes) {
        return new ArgumentSpec(false, withNullAndSorted(validTypes));
    }

    protected final ArgumentSpec optional(DataType... validTypes) {
        return new ArgumentSpec(true, withNullAndSorted(validTypes));
    }

    private Set<DataType> withNullAndSorted(DataType[] validTypes) {
        Set<DataType> realValidTypes = new LinkedHashSet<>();
        Arrays.stream(validTypes).sorted(Comparator.comparing(DataType::name)).forEach(realValidTypes::add);
        realValidTypes.add(DataTypes.NULL);
        return realValidTypes;
    }

    protected final DataType[] integers() {
        return EsqlDataTypes.types().stream().filter(DataType::isInteger).toArray(DataType[]::new);
    }

    protected final DataType[] rationals() {
        return EsqlDataTypes.types().stream().filter(DataType::isRational).toArray(DataType[]::new);
    }

    protected final DataType[] numerics() {
        return EsqlDataTypes.types().stream().filter(DataType::isNumeric).toArray(DataType[]::new);
    }

    protected record ArgumentSpec(boolean optional, Set<DataType> validTypes) {}

    protected abstract Expression build(Source source, List<Literal> args);

    protected Matcher<String> badTypeError(List<ArgumentSpec> spec, int badArgPosition, DataType badArgType) {
        String ordinal = spec.size() == 1
            ? ""
            : TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT) + " ";
        return equalTo(
            ordinal
                + "argument of [exp] must be ["
                + expectedType(spec.get(badArgPosition).validTypes())
                + "], found value [arg"
                + badArgPosition
                + "] type ["
                + badArgType.typeName()
                + "]"
        );
    }

    private String expectedType(Set<DataType> validTypes) {
        List<DataType> withoutNull = validTypes.stream().filter(t -> t != DataTypes.NULL).toList();
        if (withoutNull.size() == 1) {
            String expectedType = withoutNull.get(0).typeName();
            if (expectedType.equals("keyword")) {
                expectedType = "string";
            }
            return expectedType;
        }
        if (withoutNull.equals(Arrays.asList(integers()))) {
            return "integer";
        }
        if (withoutNull.equals(Arrays.asList(rationals()))) {
            return "double";
        }
        if (withoutNull.equals(Arrays.asList(numerics()))) {
            return "numeric";
        }
        throw new IllegalArgumentException("can't guess expected type for " + validTypes);
    }

    protected final Supplier<EvalOperator.ExpressionEvaluator> evaluator(Expression e) {
        Layout.Builder builder = new Layout.Builder();
        // Hack together a layout by scanning for Fields.
        // Those will show up in the layout in whatever order a depth first traversal finds them.
        buildLayout(builder, e);
        return EvalMapper.toEvaluator(e, builder.build());
    }

    protected final Page row(List<Object> values) {
        return new Page(BlockUtils.fromListRow(values));
    }

    private void buildLayout(Layout.Builder builder, Expression e) {
        if (e instanceof FieldAttribute f) {
            builder.appendChannel(f.id());
            return;
        }
        for (Expression c : e.children()) {
            buildLayout(builder, c);
        }
    }

    protected final FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true));
    }

    protected final void assertResolveTypeValid(Expression expression, DataType expectedType) {
        assertTrue(expression.typeResolved().resolved());
        assertThat(expressionForSimpleData().dataType(), equalTo(expectedType));
    }

    public final void testSimple() {
        List<Object> simpleData = simpleData();
        Object result = evaluator(expressionForSimpleData()).get().computeRow(row(simpleData), 0);
        assertThat(result, resultMatcher(simpleData));
    }

    public final void testSimpleWithNulls() {
        List<Object> simpleData = simpleData();
        EvalOperator.ExpressionEvaluator eval = evaluator(expressionForSimpleData()).get();
        Block[] orig = BlockUtils.fromListRow(simpleData);
        for (int i = 0; i < orig.length; i++) {
            List<Object> data = new ArrayList<>();
            Block[] blocks = new Block[orig.length];
            for (int b = 0; b < blocks.length; b++) {
                if (b == i) {
                    blocks[b] = orig[b].elementType().newBlockBuilder(1).appendNull().build();
                    data.add(null);
                } else {
                    blocks[b] = orig[b];
                    data.add(simpleData.get(b));
                }
            }
            assertSimpleWithNulls(data, eval.computeRow(new Page(blocks), 0), i);
        }
    }

    protected void assertSimpleWithNulls(List<Object> data, Object value, int nullBlock) {
        assertThat(value, nullValue());
    }

    public final void testSimpleInManyThreads() throws ExecutionException, InterruptedException {
        int count = 10_000;
        int threads = 5;
        Supplier<EvalOperator.ExpressionEvaluator> evalSupplier = evaluator(expressionForSimpleData());
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                List<Object> simpleData = simpleData();
                Page page = row(simpleData);
                Matcher<Object> resultMatcher = resultMatcher(simpleData);

                futures.add(exec.submit(() -> {
                    EvalOperator.ExpressionEvaluator eval = evalSupplier.get();
                    for (int c = 0; c < count; c++) {
                        assertThat(eval.computeRow(page, 0), resultMatcher);
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        } finally {
            exec.shutdown();
        }
    }

    public final void testEvaluatorSimpleToString() {
        assertThat(evaluator(expressionForSimpleData()).get().toString(), equalTo(expectedEvaluatorSimpleToString()));
    }

    public final void testSimpleConstantFolding() {
        List<Object> simpleData = simpleData();
        Expression e = constantFoldable(simpleData);
        assertTrue(e.foldable());
        assertThat(e.fold(), resultMatcher(simpleData));
    }

    public final void testSimpleResolveTypeValid() {
        assertResolveTypeValid(expressionForSimpleData(), expressionForSimpleDataType());
    }

    public final void testResolveType() {
        List<ArgumentSpec> specs = argSpec();
        for (int mutArg = 0; mutArg < specs.size(); mutArg++) {
            for (DataType mutArgType : EsqlDataTypes.types()) {
                List<Literal> args = new ArrayList<>(specs.size());
                for (int arg = 0; arg < specs.size(); arg++) {
                    if (mutArg == arg) {
                        args.add(new Literal(new Source(Location.EMPTY, "arg" + arg), "", mutArgType));
                    } else {
                        args.add(new Literal(new Source(Location.EMPTY, "arg" + arg), "", specs.get(arg).validTypes.iterator().next()));
                    }
                }
                assertResolution(specs, args, mutArg, mutArgType, specs.get(mutArg).validTypes.contains(mutArgType));
                int optionalIdx = specs.size() - 1;
                while (optionalIdx > 0 && specs.get(optionalIdx).optional()) {
                    args.remove(optionalIdx--);
                    assertResolution(
                        specs,
                        args,
                        mutArg,
                        mutArgType,
                        args.size() <= mutArg || specs.get(mutArg).validTypes.contains(mutArgType)
                    );
                }
            }
        }
    }

    private void assertResolution(List<ArgumentSpec> specs, List<Literal> args, int mutArg, DataType mutArgType, boolean shouldBeValid) {
        Expression exp = build(new Source(Location.EMPTY, "exp"), args);
        logger.info("checking {} is {}", exp.nodeString(), shouldBeValid ? "valid" : "invalid");
        Expression.TypeResolution resolution = exp.typeResolved();
        if (shouldBeValid) {
            assertTrue(exp.nodeString(), resolution.resolved());
        } else {
            assertFalse(exp.nodeString(), resolution.resolved());
            assertThat(exp.nodeString(), resolution.message(), badTypeError(specs, mutArg, mutArgType));
        }
    }

}
