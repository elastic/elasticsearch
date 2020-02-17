/*
* Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.function;

import org.elasticsearch.xpack.ql.session.Configuration;

import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class FunctionDefinition {
    /**
     * Converts an {@link UnresolvedFunction} into the a proper {@link Function}.
     */
    @FunctionalInterface
    public interface Builder {
        Function build(UnresolvedFunction uf, boolean distinct, Configuration configuration);
    }

    private final String name;
    private final List<String> aliases;
    private final Class<? extends Function> clazz;
    /**
     * Is this a datetime function compatible with {@code EXTRACT}.
     */
    private final boolean extractViable;
    private final Builder builder;

    FunctionDefinition(String name, List<String> aliases, Class<? extends Function> clazz, boolean datetime, Builder builder) {
        this.name = name;
        this.aliases = aliases;
        this.clazz = clazz;
        this.extractViable = datetime;
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

    Builder builder() {
        return builder;
    }

    /**
     * Is this a datetime function compatible with {@code EXTRACT}.
     */
    boolean extractViable() {
        return extractViable;
    }

    @Override
    public String toString() {
        return format(null, "{}({})", name, aliases.isEmpty() ? "" : aliases.size() == 1 ? aliases.get(0) : aliases);
    }
}
