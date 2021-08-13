/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.function;

import org.elasticsearch.xpack.ql.session.Configuration;

import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class FunctionDefinition {
    /**
     * Converts an {@link UnresolvedFunction} into the a proper {@link Function}.
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

    public FunctionDefinition(String name, List<String> aliases, Class<? extends Function> clazz, Builder builder) {
        this.name = name;
        this.aliases = aliases;
        this.clazz = clazz;
        this.builder = builder;
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

    protected Builder builder() {
        return builder;
    }

    @Override
    public String toString() {
        return format(null, "{}({})", name, aliases.isEmpty() ? "" : aliases.size() == 1 ? aliases.get(0) : aliases);
    }
}
