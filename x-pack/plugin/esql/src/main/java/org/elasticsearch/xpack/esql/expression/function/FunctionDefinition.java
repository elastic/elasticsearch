/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class FunctionDefinition {
    /**
     * Converts an {@link UnresolvedFunction} into a proper {@link Function}.
     * <p>
     * Provides the basic signature (unresolved function + runtime configuration object) while
     * allowing extensions through the vararg extras which subclasses should expand for their
     * own purposes.
     */
    @FunctionalInterface
    public interface Builder {
        Function build(UnresolvedFunction uf, Configuration configuration, Object... extras);
    }

    private final String name;
    private final List<String> aliases;
    private final Class<? extends Function> clazz;
    private final Builder builder;
    private final List<String> subCapabilities;

    public FunctionDefinition(String name, List<String> aliases, Class<? extends Function> clazz, Builder builder) {
        this(name, aliases, clazz, builder, List.of());
    }

    private FunctionDefinition(
        String name,
        List<String> aliases,
        Class<? extends Function> clazz,
        Builder builder,
        List<String> subCapabilities
    ) {
        this.name = name;
        this.aliases = aliases;
        this.clazz = clazz;
        this.builder = builder;
        this.subCapabilities = subCapabilities;
    }

    public String name() {
        return name;
    }

    public List<String> aliases() {
        return aliases;
    }

    public Class<? extends Function> clazz() {
        return clazz;
    }

    public Builder builder() {
        return builder;
    }

    public List<String> capabilities() {
        return subCapabilities;
    }

    /**
     * Adds capabilities to mark changes or fixes to the function. Use it like:
     * {@snippet :
     * public static final FunctionDefinition DEFINITION = EsqlFunctionRegistry.ternary(IpPrefix.class, IpPrefix::new, "ip_prefix")
     *     .withSubCapabilities(
     *         List.of(
     *             // Fix a bug leading to the scratch leaking data to other rows.
     *             "fix_dirty_scratch_leak"
     *         )
     *     );
     * }
     */
    public FunctionDefinition withSubCapabilities(List<String> subCapabilities) {
        return new FunctionDefinition(name, aliases, clazz, builder, subCapabilities);
    }

    @Override
    public String toString() {
        return format(null, "{}({})", name, aliases.isEmpty() ? "" : aliases.size() == 1 ? aliases.get(0) : aliases);
    }
}
