/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.function;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.Check;

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
    private static final String[] NUM_NAMES = { "zero", "one", "two", "three", "four", "five", };

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
        register(groupFunctions);
    }

    protected void register(FunctionDefinition[]... groupFunctions) {
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
                    throw new QlIllegalArgumentException(
                        "alias ["
                            + alias
                            + "] is used by "
                            + "["
                            + (old != null ? old : defs.get(alias).name())
                            + "] and ["
                            + f.name()
                            + "]"
                    );
                }
                aliases.put(alias, f.name());
            }
        }
        // sort the temporary map by key name and add it to the global map of functions
        defs.putAll(
            batchMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(
                    Collectors.<
                        Entry<String, FunctionDefinition>,
                        String,
                        FunctionDefinition,
                        LinkedHashMap<String, FunctionDefinition>>toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue,
                            LinkedHashMap::new
                        )
                )
        );
    }

    public FunctionDefinition resolveFunction(String functionName) {
        FunctionDefinition def = defs.get(functionName);
        if (def == null) {
            throw new QlIllegalArgumentException("Cannot find function {}; this should have been caught during analysis", functionName);
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
        return defs.entrySet()
            .stream()
            .filter(e -> p == null || p.matcher(e.getKey()).matches())
            .map(e -> cloneDefinition(e.getKey(), e.getValue()))
            .collect(toList());
    }

    protected FunctionDefinition cloneDefinition(String name, FunctionDefinition definition) {
        return new FunctionDefinition(name, emptyList(), definition.clazz(), definition.builder());
    }

    protected interface FunctionBuilder {
        Function build(Source source, List<Expression> children, Configuration cfg);
    }

    /**
     * Main method to register a function.
     *
     * @param names Must always have at least one entry which is the method's primary name
     */
    @SuppressWarnings("overloads")
    protected static FunctionDefinition def(Class<? extends Function> function, FunctionBuilder builder, String... names) {
        Check.isTrue(names.length > 0, "At least one name must be provided for the function");
        String primaryName = names[0];
        List<String> aliases = Arrays.asList(names).subList(1, names.length);
        FunctionDefinition.Builder realBuilder = (uf, cfg, extras) -> {
            if (CollectionUtils.isEmpty(extras) == false) {
                throw new ParsingException(
                    uf.source(),
                    "Unused parameters {} detected when building [{}]",
                    Arrays.toString(extras),
                    primaryName
                );
            }
            try {
                return builder.build(uf.source(), uf.children(), cfg);
            } catch (QlIllegalArgumentException e) {
                throw new ParsingException(e, uf.source(), "error building [{}]: {}", primaryName, e.getMessage());
            }
        };
        return new FunctionDefinition(primaryName, unmodifiableList(aliases), function, realBuilder);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a no-argument function.
     */
    protected static <T extends Function> FunctionDefinition def(
        Class<T> function,
        java.util.function.Function<Source, T> ctorRef,
        String... names
    ) {
        FunctionBuilder builder = (source, children, cfg) -> {
            if (false == children.isEmpty()) {
                throw new QlIllegalArgumentException("expects no arguments");
            }
            return ctorRef.apply(source);
        };
        return def(function, builder, names);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a unary function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(
        Class<T> function,
        BiFunction<Source, Expression, T> ctorRef,
        String... names
    ) {
        FunctionBuilder builder = (source, children, cfg) -> {
            if (children.size() != 1) {
                throw new QlIllegalArgumentException("expects exactly one argument");
            }
            return ctorRef.apply(source, children.get(0));
        };
        return def(function, builder, names);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for multi-arg/n-ary function.
     */
    @SuppressWarnings("overloads") // These are ambiguous if you aren't using ctor references but we always do
    protected <T extends Function> FunctionDefinition def(Class<T> function, NaryBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> { return ctorRef.build(source, children); };
        return def(function, builder, names);
    }

    protected interface NaryBuilder<T> {
        T build(Source source, List<Expression> children);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a binary function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function, BinaryBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> {
            boolean isBinaryOptionalParamFunction = OptionalArgument.class.isAssignableFrom(function);
            if (isBinaryOptionalParamFunction && (children.size() > 2 || children.size() < 1)) {
                throw new QlIllegalArgumentException("expects one or two arguments");
            } else if (isBinaryOptionalParamFunction == false && children.size() != 2) {
                throw new QlIllegalArgumentException("expects exactly two arguments");
            }

            return ctorRef.build(source, children.get(0), children.size() == 2 ? children.get(1) : null);
        };
        return def(function, builder, names);
    }

    protected interface BinaryBuilder<T> {
        T build(Source source, Expression left, Expression right);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a ternary function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function, TernaryBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> {
            boolean hasMinimumTwo = OptionalArgument.class.isAssignableFrom(function);
            if (hasMinimumTwo && (children.size() > 3 || children.size() < 2)) {
                throw new QlIllegalArgumentException("expects two or three arguments");
            } else if (hasMinimumTwo == false && children.size() != 3) {
                throw new QlIllegalArgumentException("expects exactly three arguments");
            }
            return ctorRef.build(source, children.get(0), children.get(1), children.size() == 3 ? children.get(2) : null);
        };
        return def(function, builder, names);
    }

    protected interface TernaryBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a quaternary function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function, QuaternaryBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> {
            if (OptionalArgument.class.isAssignableFrom(function)) {
                if (children.size() > 4 || children.size() < 3) {
                    throw new QlIllegalArgumentException("expects three or four arguments");
                }
            } else if (TwoOptionalArguments.class.isAssignableFrom(function)) {
                if (children.size() > 4 || children.size() < 2) {
                    throw new QlIllegalArgumentException("expects minimum two, maximum four arguments");
                }
            } else if (children.size() != 4) {
                throw new QlIllegalArgumentException("expects exactly four arguments");
            }
            return ctorRef.build(
                source,
                children.get(0),
                children.get(1),
                children.size() > 2 ? children.get(2) : null,
                children.size() > 3 ? children.get(3) : null
            );
        };
        return def(function, builder, names);
    }

    protected interface QuaternaryBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three, Expression four);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a quinary function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(
        Class<T> function,
        QuinaryBuilder<T> ctorRef,
        int numOptionalParams,
        String... names
    ) {
        FunctionBuilder builder = (source, children, cfg) -> {
            final int NUM_TOTAL_PARAMS = 5;
            boolean hasOptionalParams = OptionalArgument.class.isAssignableFrom(function);
            if (hasOptionalParams && (children.size() > NUM_TOTAL_PARAMS || children.size() < NUM_TOTAL_PARAMS - numOptionalParams)) {
                throw new QlIllegalArgumentException(
                    "expects between "
                        + NUM_NAMES[NUM_TOTAL_PARAMS - numOptionalParams]
                        + " and "
                        + NUM_NAMES[NUM_TOTAL_PARAMS]
                        + " arguments"
                );
            } else if (hasOptionalParams == false && children.size() != NUM_TOTAL_PARAMS) {
                throw new QlIllegalArgumentException("expects exactly " + NUM_NAMES[NUM_TOTAL_PARAMS] + " arguments");
            }
            return ctorRef.build(
                source,
                children.size() > 0 ? children.get(0) : null,
                children.size() > 1 ? children.get(1) : null,
                children.size() > 2 ? children.get(2) : null,
                children.size() > 3 ? children.get(3) : null,
                children.size() > 4 ? children.get(4) : null
            );
        };
        return def(function, builder, names);
    }

    protected interface QuinaryBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three, Expression four, Expression five);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for functions with a mandatory argument followed by a varidic list.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function, UnaryVariadicBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> {
            boolean hasMinimumOne = OptionalArgument.class.isAssignableFrom(function);
            if (hasMinimumOne && children.size() < 1) {
                throw new QlIllegalArgumentException("expects at least one argument");
            } else if (hasMinimumOne == false && children.size() < 2) {
                throw new QlIllegalArgumentException("expects at least two arguments");
            }
            return ctorRef.build(source, children.get(0), children.subList(1, children.size()));
        };
        return def(function, builder, names);
    }

    protected interface UnaryVariadicBuilder<T> {
        T build(Source source, Expression exp, List<Expression> variadic);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a no-argument function that is configuration aware.
     */
    @SuppressWarnings("overloads")
    protected static <T extends Function> FunctionDefinition def(Class<T> function, ConfigurationAwareBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> {
            if (false == children.isEmpty()) {
                throw new QlIllegalArgumentException("expects no arguments");
            }
            return ctorRef.build(source, cfg);
        };
        return def(function, builder, names);
    }

    protected interface ConfigurationAwareBuilder<T> {
        T build(Source source, Configuration configuration);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a one-argument function that is configuration aware.
     */
    @SuppressWarnings("overloads")
    protected static <T extends Function> FunctionDefinition def(
        Class<T> function,
        UnaryConfigurationAwareBuilder<T> ctorRef,
        String... names
    ) {
        FunctionBuilder builder = (source, children, cfg) -> {
            if (children.size() > 1) {
                throw new QlIllegalArgumentException("expects exactly one argument");
            }
            Expression ex = children.size() == 1 ? children.get(0) : null;
            return ctorRef.build(source, ex, cfg);
        };
        return def(function, builder, names);
    }

    protected interface UnaryConfigurationAwareBuilder<T> {
        T build(Source source, Expression exp, Configuration configuration);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a binary function that is configuration aware.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(
        Class<T> function,
        BinaryConfigurationAwareBuilder<T> ctorRef,
        String... names
    ) {
        FunctionBuilder builder = (source, children, cfg) -> {
            if (children.size() != 2) {
                throw new QlIllegalArgumentException("expects exactly two arguments");
            }
            return ctorRef.build(source, children.get(0), children.get(1), cfg);
        };
        return def(function, builder, names);
    }

    protected interface BinaryConfigurationAwareBuilder<T> {
        T build(Source source, Expression left, Expression right, Configuration configuration);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a ternary function that is configuration aware.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected <T extends Function> FunctionDefinition def(Class<T> function, TernaryConfigurationAwareBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> {
            boolean hasMinimumTwo = OptionalArgument.class.isAssignableFrom(function);
            if (hasMinimumTwo && (children.size() > 3 || children.size() < 2)) {
                throw new QlIllegalArgumentException("expects two or three arguments");
            } else if (hasMinimumTwo == false && children.size() != 3) {
                throw new QlIllegalArgumentException("expects exactly three arguments");
            }
            return ctorRef.build(source, children.get(0), children.get(1), children.size() == 3 ? children.get(2) : null, cfg);
        };
        return def(function, builder, names);
    }

    protected interface TernaryConfigurationAwareBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three, Configuration configuration);
    }

    //
    // Utility method for extra argument extraction.
    //
    protected static Boolean asBool(Object[] extras) {
        if (CollectionUtils.isEmpty(extras)) {
            return null;
        }
        if (extras.length != 1 || (extras[0] instanceof Boolean) == false) {
            throw new QlIllegalArgumentException("Invalid number and types of arguments given to function definition");
        }
        return (Boolean) extras[0];
    }

}
