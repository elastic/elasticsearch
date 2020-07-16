/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.script.Script;

import java.util.Objects;

/**
 * Abstract base class for building queries based on script fields.
 */
abstract class AbstractScriptFieldQuery extends Query {
    private final Script script;
    private final String fieldName;

    AbstractScriptFieldQuery(Script script, String fieldName) {
        this.script = Objects.requireNonNull(script);
        this.fieldName = Objects.requireNonNull(fieldName);
    }

    final Script script() {
        return script;
    }

    final String fieldName() {
        return fieldName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), script, fieldName);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AbstractScriptFieldQuery other = (AbstractScriptFieldQuery) obj;
        return script.equals(other.script) && fieldName.equals(other.fieldName);
    }

}
