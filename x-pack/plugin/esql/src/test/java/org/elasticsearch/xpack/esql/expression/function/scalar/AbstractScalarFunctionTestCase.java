/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Base class for function tests.
 */
public abstract class AbstractScalarFunctionTestCase extends AbstractFunctionTestCase {
    /**
     * Describe supported arguments. Build each argument with
     * {@link #required} or {@link #optional}.
     */
    protected abstract List<ArgumentSpec> argSpec();

    /**
     * The data type that applying this function to arguments of this type should produce.
     */
    protected abstract DataType expectedType(List<DataType> argTypes);

    /**
     * Define a required argument.
     */
    protected final ArgumentSpec required(DataType... validTypes) {
        return new ArgumentSpec(false, withNullAndSorted(validTypes));
    }

    /**
     * Define an optional argument.
     */
    protected final ArgumentSpec optional(DataType... validTypes) {
        return new ArgumentSpec(true, withNullAndSorted(validTypes));
    }

    private Set<DataType> withNullAndSorted(DataType[] validTypes) {
        Set<DataType> realValidTypes = new LinkedHashSet<>();
        Arrays.stream(validTypes).sorted(Comparator.comparing(DataType::name)).forEach(realValidTypes::add);
        realValidTypes.add(DataTypes.NULL);
        return realValidTypes;
    }

    /**
     * All string types (keyword, text, match_only_text, etc). For passing to {@link #required} or {@link #optional}.
     */
    protected final DataType[] strings() {
        return EsqlDataTypes.types().stream().filter(DataTypes::isString).toArray(DataType[]::new);
    }

    /**
     * All integer types (long, int, short, byte). For passing to {@link #required} or {@link #optional}.
     */
    protected final DataType[] integers() {
        return EsqlDataTypes.types().stream().filter(DataType::isInteger).toArray(DataType[]::new);
    }

    /**
     * All rational types (double, float, whatever). For passing to {@link #required} or {@link #optional}.
     */
    protected final DataType[] rationals() {
        return EsqlDataTypes.types().stream().filter(DataType::isRational).toArray(DataType[]::new);
    }

    /**
     * All numeric types (integers and rationals.) For passing to {@link #required} or {@link #optional}.
     */
    protected final DataType[] numerics() {
        return EsqlDataTypes.types().stream().filter(DataType::isNumeric).toArray(DataType[]::new);
    }

    protected final DataType[] representableNumerics() {
        // TODO numeric should only include representable numbers but that is a change for a followup
        return EsqlDataTypes.types().stream().filter(DataType::isNumeric).filter(EsqlDataTypes::isRepresentable).toArray(DataType[]::new);
    }

    protected final DataType[] representable() {
        return EsqlDataTypes.types().stream().filter(EsqlDataTypes::isRepresentable).toArray(DataType[]::new);
    }

    protected record ArgumentSpec(boolean optional, Set<DataType> validTypes) {}

    @Override
    protected final DataType expressionForSimpleDataType() {
        return expectedType(expressionForSimpleData().children().stream().map(e -> e.dataType()).toList());
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
        if (shouldBeValid) {
            assertResolveTypeValid(exp, expectedType(args.stream().map(Expression::dataType).toList()));
            return;
        }
        Expression.TypeResolution resolution = exp.typeResolved();
        assertFalse(exp.nodeString(), resolution.resolved());
        assertThat(exp.nodeString(), resolution.message(), badTypeError(specs, mutArg, mutArgType));
    }

    protected Matcher<String> badTypeError(List<ArgumentSpec> spec, int badArgPosition, DataType badArgType) {
        String ordinal = spec.size() == 1
            ? ""
            : TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT) + " ";
        return equalTo(
            ordinal
                + "argument of [exp] must be ["
                + expectedTypeName(spec.get(badArgPosition).validTypes())
                + "], found value [arg"
                + badArgPosition
                + "] type ["
                + badArgType.typeName()
                + "]"
        );
    }

    private String expectedTypeName(Set<DataType> validTypes) {
        List<DataType> withoutNull = validTypes.stream().filter(t -> t != DataTypes.NULL).toList();
        if (withoutNull.equals(Arrays.asList(strings()))) {
            return "string";
        }
        if (withoutNull.equals(Arrays.asList(integers()))) {
            return "integer";
        }
        if (withoutNull.equals(Arrays.asList(rationals()))) {
            return "double";
        }
        if (withoutNull.equals(Arrays.asList(numerics())) || withoutNull.equals(Arrays.asList(representableNumerics()))) {
            return "numeric";
        }
        if (validTypes.equals(Set.copyOf(Arrays.asList(representable())))) {
            return "representable";
        }
        throw new IllegalArgumentException("can't guess expected type for " + validTypes);
    }
}
