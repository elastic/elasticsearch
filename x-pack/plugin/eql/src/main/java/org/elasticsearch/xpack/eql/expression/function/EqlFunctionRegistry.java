/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function;

import org.elasticsearch.xpack.eql.expression.function.scalar.math.ToNumber;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Between;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.CIDRMatch;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.IndexOf;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.StringContains;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.ToString;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.string.CaseInsensitiveScalarFunction;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.Check;

import java.util.List;
import java.util.Locale;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public class EqlFunctionRegistry extends FunctionRegistry {

    public EqlFunctionRegistry() {
        register(functions());
    }

    EqlFunctionRegistry(FunctionDefinition... functions) {
        register(functions);
    }

    private FunctionDefinition[][] functions() {
        return new FunctionDefinition[][] {
            // Scalar functions
            // String
            new FunctionDefinition[] {
                def(Between.class, Between::new, "between"),
                def(CIDRMatch.class, CIDRMatch::new, "cidrmatch"),
                def(Concat.class, Concat::new, "concat"),
                def(EndsWith.class, EndsWith::new, "endswith"),
                def(IndexOf.class, IndexOf::new, "indexof"),
                def(Length.class, Length::new, "length"),
                def(StartsWith.class, StartsWith::new, "startswith"),
                def(ToString.class, ToString::new, "string"),
                def(StringContains.class, StringContains::new, "stringcontains"),
                def(Substring.class, Substring::new, "substring"), },
            // Arithmetic
            new FunctionDefinition[] {
                def(Add.class, Add::new, "add"),
                def(Div.class, Div::new, "divide"),
                def(Mod.class, Mod::new, "modulo"),
                def(Mul.class, Mul::new, "multiply"),
                def(ToNumber.class, ToNumber::new, "number"),
                def(Sub.class, Sub::new, "subtract"), } };
    }

    @Override
    protected String normalize(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    /**
     * Builder for creating EQL-specific functions.
     * All other methods defined here end up being translated to this form.
     */
    protected interface EqlFunctionBuilder {
        Function build(Source source, List<Expression> children, Boolean caseInsensitive);
    }

    /**
     * Main method to register a function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static FunctionDefinition def(Class<? extends Function> function, EqlFunctionBuilder builder, String... names) {
        Check.isTrue(names.length > 0, "At least one name must be provided for the function");
        String primaryName = names[0];
        List<String> aliases = asList(names).subList(1, names.length);
        FunctionDefinition.Builder realBuilder = (uf, cfg, extras) -> {
            try {
                return builder.build(uf.source(), uf.children(), asBool(extras));
            } catch (QlIllegalArgumentException e) {
                throw new ParsingException(uf.source(), "error building [" + primaryName + "]: " + e.getMessage(), e);
            }
        };
        boolean caseAware = CaseInsensitiveScalarFunction.class.isAssignableFrom(function);
        return new EqlFunctionDefinition(primaryName, unmodifiableList(aliases), function, caseAware, realBuilder);
    }

    protected interface BinaryCaseAwareBuilder<T> {
        T build(Source source, Expression left, Expression right, boolean caseInsensitive);
    }

    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function, BinaryCaseAwareBuilder<T> ctorRef, String... names) {
        EqlFunctionBuilder builder = (source, children, caseInsensitive) -> {
            if (children.size() != 2) {
                throw new QlIllegalArgumentException("expects exactly two arguments");
            }
            return ctorRef.build(source, children.get(0), children.get(1), defaultSensitivityIfNotSet(caseInsensitive));
        };
        return def(function, builder, names);
    }

    private static Boolean defaultSensitivityIfNotSet(Boolean caseInsensitive) {
        return caseInsensitive == null ? Boolean.FALSE : caseInsensitive;
    }

    protected interface TernaryCaseAwareBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three, boolean caseInsensitive);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a ternary function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function, TernaryCaseAwareBuilder<T> ctorRef, String... names) {
        EqlFunctionBuilder builder = (source, children, caseInsensitive) -> {
            boolean hasMinimumTwo = OptionalArgument.class.isAssignableFrom(function);
            if (hasMinimumTwo && (children.size() > 3 || children.size() < 2)) {
                throw new QlIllegalArgumentException("expects two or three arguments");
            } else if (hasMinimumTwo == false && children.size() != 3) {
                throw new QlIllegalArgumentException("expects exactly three arguments");
            }
            return ctorRef.build(
                source,
                children.get(0),
                children.get(1),
                children.size() == 3 ? children.get(2) : null,
                defaultSensitivityIfNotSet(caseInsensitive)
            );
        };
        return def(function, builder, names);
    }

    protected interface QuaternaryBuilderCaseAwareBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three, Expression four, boolean caseInsensitive);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a quaternary function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(
        Class<T> function,
        QuaternaryBuilderCaseAwareBuilder<T> ctorRef,
        String... names
    ) {
        EqlFunctionBuilder builder = (source, children, caseInsensitive) -> {
            boolean hasMinimumThree = OptionalArgument.class.isAssignableFrom(function);
            if (hasMinimumThree && (children.size() > 4 || children.size() < 3)) {
                throw new QlIllegalArgumentException("expects three or four arguments");
            } else if (hasMinimumThree == false && children.size() != 4) {
                throw new QlIllegalArgumentException("expects exactly four arguments");
            }
            return ctorRef.build(
                source,
                children.get(0),
                children.get(1),
                children.get(2),
                children.size() == 4 ? children.get(3) : null,
                defaultSensitivityIfNotSet(caseInsensitive)
            );
        };
        return def(function, builder, names);
    }

    protected interface UnaryVariadicCaseAwareBuilder<T> {
        T build(Source source, Expression exp, List<Expression> variadic, boolean caseInsensitive);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a quaternary function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(
        Class<T> function,
        UnaryVariadicCaseAwareBuilder<T> ctorRef,
        String... names
    ) {
        EqlFunctionBuilder builder = (source, children, caseInsensitive) -> {
            if (children.size() < 2) {
                throw new QlIllegalArgumentException("expects at least two arguments");
            }
            return ctorRef.build(
                source,
                children.get(0),
                children.subList(1, children.size()),
                defaultSensitivityIfNotSet(caseInsensitive)
            );
        };
        return def(function, builder, names);
    }
}
