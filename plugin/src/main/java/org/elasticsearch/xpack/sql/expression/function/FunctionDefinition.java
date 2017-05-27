/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;

public class FunctionDefinition {

    private final String name;
    private final List<String> aliases;
    private final Class<? extends Function> clazz;
    private final FunctionType type;

    FunctionDefinition(String name, List<String> aliases, Class<? extends Function> clazz) {
        this.name = name;
        this.aliases = aliases;
        this.clazz = clazz;
        this.type = FunctionType.of(clazz);
    }

    public String name() {
        return name;
    }

    List<String> aliases() {
        return aliases;
    }

    public FunctionType type() {
        return type;
    }

    Class<? extends Function> clazz() {
        return clazz;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clazz);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        FunctionDefinition other = (FunctionDefinition) obj;
        return Objects.equals(clazz, other.clazz) &&
                Objects.equals(name, other.name) &&
                Objects.equals(aliases, aliases);
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "%s(%s)", name, aliases.isEmpty() ? "" : aliases.size() == 1 ? aliases.get(0) : aliases );
    }
}
