/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;

/**
 * Base class holding common properties for Elasticsearch aggregations.
 */
public abstract class Agg {

    private final String id;
    private final String fieldName;
    private final ScriptTemplate scriptTemplate;

    Agg(String id, Object fieldOrScript) {
        this.id = id;
        if (fieldOrScript instanceof String) {
            this.fieldName = (String) fieldOrScript;
            this.scriptTemplate = null;
        } else if (fieldOrScript instanceof ScriptTemplate) {
            this.fieldName = null;
            this.scriptTemplate = (ScriptTemplate) fieldOrScript;
        } else {
            throw new SqlIllegalArgumentException("Argument of an aggregate function should be String or ScriptTemplate");
        }
    }

    public String id() {
        return id;
    }

    protected String fieldName() {
        return fieldName;
    }

    public ScriptTemplate scriptTemplate() {
        return scriptTemplate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, fieldName, scriptTemplate);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Agg other = (Agg) obj;
        return Objects.equals(id, other.id)
            && Objects.equals(fieldName, other.fieldName)
            && Objects.equals(scriptTemplate, other.scriptTemplate);
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "%s(%s)", getClass().getSimpleName(), fieldName != null ? fieldName : scriptTemplate.toString());
    }
}
