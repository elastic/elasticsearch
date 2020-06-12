/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.function;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.util.Check;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class FunctionRegistry {

    // Translation table for error messaging in the following function
    private static final String[] NUM_NAMES = {
            "zero",
            "one",
            "two",
            "three",
            "four",
            "five",
    };

    // list of functions grouped by type of functions (aggregate, statistics, math etc) and ordered alphabetically inside each group
    // a single function will have one entry for itself with its name associated to its instance and, also, one entry for each alias
    // it has with the alias name associated to the FunctionDefinition instance
    private final Map<String, FunctionDefinition> defs = new LinkedHashMap<>();
    private final Map<String, String> aliases = new HashMap<>();

    public FunctionRegistry() {}

    /**
     * Register the given function definitions with this registry.
     */
    public FunctionRegistry(FunctionDefinition... functions) {
        register(functions);
    }

    public FunctionRegistry(FunctionDefinition[]... groupFunctions) {
        for (FunctionDefinition[] group : groupFunctions) {
            register(group);
        }
    }

    protected void register(FunctionDefinition... functions) {
        // temporary map to hold [function_name/alias_name : function instance]
        Map<String, FunctionDefinition> batchMap = new HashMap<>();
        for (FunctionDefinition f : functions) {
            batchMap.put(f.name(), f);
            for (String alias : f.aliases()) {
                Object old = batchMap.put(alias, f);
                if (old != null || defs.containsKey(alias)) {
                    throw new QlIllegalArgumentException("alias [" + alias + "] is used by "
                            + "[" + (old != null ? old : defs.get(alias).name()) + "] and [" + f.name() + "]");
                }
                aliases.put(alias, f.name());
            }
        }
        // sort the temporary map by key name and add it to the global map of functions
        defs.putAll(batchMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.<Entry<String, FunctionDefinition>, String,
                        FunctionDefinition, LinkedHashMap<String, FunctionDefinition>> toMap(Map.Entry::getKey, Map.Entry::getValue,
                (oldValue, newValue) -> oldValue, LinkedHashMap::new)));
    }

    public FunctionDefinition resolveFunction(String functionName) {
        FunctionDefinition def = defs.get(functionName);
        if (def == null) {
            throw new QlIllegalArgumentException(
                "Cannot find function {}; this should have been caught during analysis",
                functionName);
        }
        return def;
    }

    protected String normalize(String name) {
        return name.toUpperCase(Locale.ROOT);
    }

    public String resolveAlias(String alias) {
        String normalized = normalize(alias);
        return aliases.getOrDefault(normalized, normalized);
    }

    public boolean functionExists(String functionName) {
        return defs.containsKey(functionName);
    }

    public Collection<FunctionDefinition> listFunctions() {
        // It is worth double checking if we need this copy. These are immutable anyway.
        return defs.values();
    }

    public Collection<FunctionDefinition> listFunctions(String pattern) {
        // It is worth double checking if we need this copy. These are immutable anyway.
        Pattern p = Strings.hasText(pattern) ? Pattern.compile(normalize(pattern)) : null;
        return defs.entrySet().stream()
                .filter(e -> p == null || p.matcher(e.getKey()).matches())
                .map(e -> new FunctionDefinition(e.getKey(), emptyList(),
                        e.getValue().clazz(), e.getValue().extractViable(), e.getValue().builder()))
                .collect(toList());
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a no-argument function that
     * is not aware of time zone and does not support {@code DISTINCT}.
     */
    protected static <T extends Function> FunctionDefinition def(Class<T> function,
            java.util.function.Function<Source, T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (false == children.isEmpty()) {
                throw new QlIllegalArgumentException("expects no arguments");
            }
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.apply(source);
        };
        return def(function, builder, false, names);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a no-argument function that
     * is not aware of time zone, does not support {@code DISTINCT} and needs
     * the cluster name (DATABASE()) or the user name (USER()).
     */
    @SuppressWarnings("overloads")
    protected static <T extends Function> FunctionDefinition def(Class<T> function,
            ConfigurationAwareFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (false == children.isEmpty()) {
                throw new QlIllegalArgumentException("expects no arguments");
            }
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, cfg);
        };
        return def(function, builder, false, names);
    }

    protected interface ConfigurationAwareFunctionBuilder<T> {
        T build(Source source, Configuration configuration);
    }

    /**
    * Build a {@linkplain FunctionDefinition} for a one-argument function that
    * is not aware of time zone, does not support {@code DISTINCT} and needs
    * the configuration object.
    */
    @SuppressWarnings("overloads")
    protected static <T extends Function> FunctionDefinition def(Class<T> function,
            UnaryConfigurationAwareFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() > 1) {
                throw new QlIllegalArgumentException("expects exactly one argument");
            }
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            Expression ex = children.size() == 1 ? children.get(0) : null;
            return ctorRef.build(source, ex, cfg);
        };
        return def(function, builder, false, names);
    }

    protected interface UnaryConfigurationAwareFunctionBuilder<T> {
        T build(Source source, Expression exp, Configuration configuration);
    }


    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that is not
     * aware of time zone and does not support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function,
            BiFunction<Source, Expression, T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() != 1) {
                throw new QlIllegalArgumentException("expects exactly one argument");
            }
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.apply(source, children.get(0));
        };
        return def(function, builder, false, names);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for multi-arg function that
     * is not aware of time zone and does not support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads") // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function,
            MultiFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children);
        };
        return def(function, builder, false, names);
    }

    protected interface MultiFunctionBuilder<T> {
        T build(Source source, List<Expression> children);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that is not
     * aware of time zone but does support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function,
            DistinctAwareUnaryFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() != 1) {
                throw new QlIllegalArgumentException("expects exactly one argument");
            }
            return ctorRef.build(source, children.get(0), distinct);
        };
        return def(function, builder, false, names);
    }

    protected interface DistinctAwareUnaryFunctionBuilder<T> {
        T build(Source source, Expression target, boolean distinct);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that
     * operates on a datetime.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function,
            DatetimeUnaryFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() != 1) {
                throw new QlIllegalArgumentException("expects exactly one argument");
            }
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), cfg.zoneId());
        };
        return def(function, builder, true, names);
    }

    protected interface DatetimeUnaryFunctionBuilder<T> {
        T build(Source source, Expression target, ZoneId zi);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a binary function that
     * requires a timezone.
     */
    @SuppressWarnings("overloads") // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function, DatetimeBinaryFunctionBuilder<T> ctorRef,
            String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() != 2) {
                throw new QlIllegalArgumentException("expects exactly two arguments");
            }
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), children.get(1), cfg.zoneId());
        };
        return def(function, builder, false, names);
    }

    protected interface DatetimeBinaryFunctionBuilder<T> {
        T build(Source source, Expression lhs, Expression rhs, ZoneId zi);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a three-args function that
     * requires a timezone.
     */
    @SuppressWarnings("overloads") // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function, DatetimeThreeArgsFunctionBuilder<T> ctorRef,
            String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() != 3) {
                throw new QlIllegalArgumentException("expects three arguments");
            }
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), children.get(1), children.get(2), cfg.zoneId());
        };
        return def(function, builder, false, names);
    }

    protected interface DatetimeThreeArgsFunctionBuilder<T> {
        T build(Source source, Expression first, Expression second, Expression third, ZoneId zi);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a binary function that is
     * not aware of time zone and does not support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function,
            BinaryFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            boolean isBinaryOptionalParamFunction = OptionalArgument.class.isAssignableFrom(function);
            if (isBinaryOptionalParamFunction && (children.size() > 2 || children.size() < 1)) {
                throw new QlIllegalArgumentException("expects one or two arguments");
            } else if (!isBinaryOptionalParamFunction && children.size() != 2) {
                throw new QlIllegalArgumentException("expects exactly two arguments");
            }

            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), children.size() == 2 ? children.get(1) : null);
        };
        return def(function, builder, false, names);
    }

    protected interface BinaryFunctionBuilder<T> {
        T build(Source source, Expression lhs, Expression rhs);
    }

    /**
     * Main method to register a function/
     * @param names Must always have at least one entry which is the method's primary name
     *
     */
    @SuppressWarnings("overloads")
    public static FunctionDefinition def(Class<? extends Function> function, FunctionBuilder builder,
                                          boolean datetime, String... names) {
        Check.isTrue(names.length > 0, "At least one name must be provided for the function");
        String primaryName = names[0];
        List<String> aliases = Arrays.asList(names).subList(1, names.length);
        FunctionDefinition.Builder realBuilder = (uf, distinct, cfg) -> {
            try {
                return builder.build(uf.source(), uf.children(), distinct, cfg);
            } catch (QlIllegalArgumentException e) {
                throw new ParsingException(uf.source(), "error building [" + primaryName + "]: " + e.getMessage(), e);
            }
        };
        return new FunctionDefinition(primaryName, unmodifiableList(aliases), function, datetime, realBuilder);
    }

    public interface FunctionBuilder {
        Function build(Source source, List<Expression> children, boolean distinct, Configuration cfg);
    }

    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function,
            ThreeParametersFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            boolean hasMinimumTwo = OptionalArgument.class.isAssignableFrom(function);
            if (hasMinimumTwo && (children.size() > 3 || children.size() < 2)) {
                throw new QlIllegalArgumentException("expects two or three arguments");
            } else if (!hasMinimumTwo && children.size() != 3) {
                throw new QlIllegalArgumentException("expects exactly three arguments");
            }
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), children.get(1), children.size() == 3 ? children.get(2) : null);
        };
        return def(function, builder, false, names);
    }

    protected interface ThreeParametersFunctionBuilder<T> {
        T build(Source source, Expression src, Expression exp1, Expression exp2);
    }

    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function,
                                                              ScalarTriFunctionConfigurationAwareBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            boolean hasMinimumTwo = OptionalArgument.class.isAssignableFrom(function);
            if (hasMinimumTwo && (children.size() > 3 || children.size() < 2)) {
                throw new QlIllegalArgumentException("expects two or three arguments");
            } else if (!hasMinimumTwo && children.size() != 3) {
                throw new QlIllegalArgumentException("expects exactly three arguments");
            }
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), children.get(1), children.size() == 3 ? children.get(2) : null, cfg);
        };
        return def(function, builder, false, names);
    }

    protected interface ScalarTriFunctionConfigurationAwareBuilder<T> {
        T build(Source source, Expression exp1, Expression exp2, Expression exp3, Configuration configuration);
    }

    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function,
            FourParametersFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() != 4) {
                throw new QlIllegalArgumentException("expects exactly four arguments");
            }
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), children.get(1), children.get(2), children.get(3));
        };
        return def(function, builder, false, names);
    }

    protected interface FourParametersFunctionBuilder<T> {
        T build(Source source, Expression src, Expression exp1, Expression exp2, Expression exp3);
    }

    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function,
                                                              FiveParametersFunctionBuilder<T> ctorRef,
                                                              int numOptionalParams, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            final int NUM_TOTAL_PARAMS = 5;
            boolean hasOptionalParams = OptionalArgument.class.isAssignableFrom(function);
            if (hasOptionalParams && (children.size() > NUM_TOTAL_PARAMS || children.size() < NUM_TOTAL_PARAMS - numOptionalParams)) {
                throw new QlIllegalArgumentException("expects between " + NUM_NAMES[NUM_TOTAL_PARAMS - numOptionalParams]
                        + " and " + NUM_NAMES[NUM_TOTAL_PARAMS] + " arguments");
            } else if (hasOptionalParams == false && children.size() != NUM_TOTAL_PARAMS) {
                throw new QlIllegalArgumentException("expects exactly " + NUM_NAMES[NUM_TOTAL_PARAMS] + " arguments");
            }
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source,
                    children.size() > 0 ? children.get(0) : null,
                    children.size() > 1 ? children.get(1) : null,
                    children.size() > 2 ? children.get(2) : null,
                    children.size() > 3 ? children.get(3) : null,
                    children.size() > 4 ? children.get(4) : null);
        };
        return def(function, builder, false, names);
    }

    protected interface FiveParametersFunctionBuilder<T> {
        T build(Source source, Expression src, Expression exp1, Expression exp2, Expression exp3, Expression exp4);
    }

    /**
     * Special method to create function definition for Cast as its
     * signature is not compatible with {@link UnresolvedFunction}
     *
     * @return Cast function definition
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function,
                                                               CastFunctionBuilder<T> ctorRef,
                                                               String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) ->
            ctorRef.build(source, children.get(0), children.get(0).dataType());
        return def(function, builder, false, names);
    }

    protected interface CastFunctionBuilder<T> {
        T build(Source source, Expression expression, DataType dataType);
    }

    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function,
                                                              TwoParametersVariadicBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            boolean hasMinimumOne = OptionalArgument.class.isAssignableFrom(function);
            if (hasMinimumOne && children.size() < 1) {
                throw new QlIllegalArgumentException("expects at least one argument");
            } else if (!hasMinimumOne && children.size() < 2) {
                throw new QlIllegalArgumentException("expects at least two arguments");
            }
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), children.subList(1, children.size()));
        };
        return def(function, builder, false, names);
    }

    protected interface TwoParametersVariadicBuilder<T> {
        T build(Source source, Expression src, List<Expression> remaining);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a binary function that is case sensitive aware.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function,
        ScalarBiFunctionConfigurationAwareBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() != 2) {
                throw new QlIllegalArgumentException("expects exactly two arguments");
            }
            if (distinct) {
                throw new QlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), children.get(1), cfg);
        };
        return def(function, builder, true, names);
    }

    protected interface ScalarBiFunctionConfigurationAwareBuilder<T> {
        T build(Source source, Expression e1, Expression e2, Configuration configuration);
    }
}
