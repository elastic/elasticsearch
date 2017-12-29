/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.util.StringUtils;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

abstract class AbstractFunctionRegistry implements FunctionRegistry {
    private final Map<String, FunctionDefinition> defs = new LinkedHashMap<>();
    private final Map<String, String> aliases;

    protected AbstractFunctionRegistry(List<FunctionDefinition> functions) {
        this.aliases = new HashMap<>();
        for (FunctionDefinition f : functions) {
            defs.put(f.name(), f);
            for (String alias : f.aliases()) {
                Object old = aliases.put(alias, f.name());
                if (old != null) {
                    throw new IllegalArgumentException("alias [" + alias + "] is used by [" + old + "] and [" + f.name() + "]");
                }
                defs.put(alias, f);
            }
        }
    }

    @Override
    public Function resolveFunction(UnresolvedFunction ur, DateTimeZone timeZone) {
        FunctionDefinition def = defs.get(normalize(ur.name()));
        if (def == null) {
            throw new SqlIllegalArgumentException("Cannot find function %s; this should have been caught during analysis", ur.name());
        }
        return def.builder().apply(ur, timeZone);
    }

    @Override
    public String concreteFunctionName(String alias) {
        String normalized = normalize(alias);
        return aliases.getOrDefault(normalized, normalized);
    }

    @Override
    public boolean functionExists(String name) {
        return defs.containsKey(normalize(name));
    }

    @Override
    public Collection<FunctionDefinition> listFunctions() {
        return defs.entrySet().stream()
                .map(e -> new FunctionDefinition(e.getKey(), emptyList(), e.getValue().clazz(), e.getValue().builder()))
                .collect(toList());
    }

    @Override
    public Collection<FunctionDefinition> listFunctions(String pattern) {
        Pattern p = Strings.hasText(pattern) ? StringUtils.likeRegex(normalize(pattern)) : null;
        return defs.entrySet().stream()
                .filter(e -> p == null || p.matcher(e.getKey()).matches())
                .map(e -> new FunctionDefinition(e.getKey(), emptyList(), e.getValue().clazz(), e.getValue().builder()))
                .collect(toList());
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a no-argument function that
     * is not aware of time zone and does not support {@code DISTINCT}.
     */
    protected static <T extends Function> FunctionDefinition def(Class<T> function,
            java.util.function.Function<Location, T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, tz) -> {
            if (false == children.isEmpty()) {
                throw new IllegalArgumentException("expects only a single argument");
            }
            if (distinct) {
                throw new IllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.apply(location);
        };
        return def(function, builder, aliases);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that is not
     * aware of time zone and does not support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function,
            BiFunction<Location, Expression, T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, tz) -> {
            if (children.size() != 1) {
                throw new IllegalArgumentException("expects only a single argument");
            }
            if (distinct) {
                throw new IllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.apply(location, children.get(0));
        };
        return def(function, builder, aliases);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that is not
     * aware of time zone but does support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function,
            DistinctAwareUnaryFunctionBuilder<T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, tz) -> {
            if (children.size() != 1) {
                throw new IllegalArgumentException("expects only a single argument");
            }
            return ctorRef.build(location, children.get(0), distinct);
        };
        return def(function, builder, aliases);
    }
    protected interface DistinctAwareUnaryFunctionBuilder<T> {
        T build(Location location, Expression target, boolean distinct);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that is
     * aware of time zone and does not support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function,
            TimeZoneAwareUnaryFunctionBuilder<T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, tz) -> {
            if (children.size() != 1) {
                throw new IllegalArgumentException("expects only a single argument");
            }
            if (distinct) {
                throw new IllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(location, children.get(0), tz);
        };
        return def(function, builder, aliases);
    }
    protected interface TimeZoneAwareUnaryFunctionBuilder<T> {
        T build(Location location, Expression target, DateTimeZone tz);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a binary function that is
     * not aware of time zone and does not support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function,
            BinaryFunctionBuilder<T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, tz) -> {
            if (children.size() != 2) {
                throw new IllegalArgumentException("expects only a single argument");
            }
            if (distinct) {
                throw new IllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(location, children.get(0), children.get(1));
        };
        return def(function, builder, aliases);
    }
    protected interface BinaryFunctionBuilder<T> {
        T build(Location location, Expression lhs, Expression rhs);
    }

    private static FunctionDefinition def(Class<? extends Function> function, FunctionBuilder builder, String... aliases) {
        String primaryName = normalize(function.getSimpleName());
        BiFunction<UnresolvedFunction, DateTimeZone, Function> realBuilder = (uf, tz) -> {
            try {
                return builder.build(uf.location(), uf.children(), uf.distinct(), tz);
            } catch (IllegalArgumentException e) {
                throw new ParsingException("error builder [" + primaryName + "]: " + e.getMessage(), e,
                        uf.location().getLineNumber(), uf.location().getColumnNumber());
            }
        };
        return new FunctionDefinition(primaryName, unmodifiableList(Arrays.asList(aliases)), function, realBuilder);
    }
    private interface FunctionBuilder {
        Function build(Location location, List<Expression> children, boolean distinct, DateTimeZone tz);
    }

    protected static String normalize(String name) {
        // translate CamelCase to camel_case
        return StringUtils.camelCaseToUnderscore(name);
    }
}
