/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.script;

import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;

public class ScriptFunction {

    final String name;
    final String definition;

    public ScriptFunction(String name, String definition) {
        this.name = name;
        this.definition = definition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, definition);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ScriptFunction other = (ScriptFunction) obj;
        return Objects.equals(name, other.name) && Objects.equals(definition, other.definition);
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "%s=%s", name, definition);
    }
}