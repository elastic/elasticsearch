/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;

import java.util.Objects;

public class AggSource {

    private final String fieldName;
    private final ScriptTemplate script;

    private AggSource(String fieldName, ScriptTemplate script) {
        this.fieldName = fieldName;
        this.script = script;
    }

    public static AggSource of(String fieldName) {
        return new AggSource(fieldName, null);
    }

    public static AggSource of(ScriptTemplate script) {
        return new AggSource(null, script);
    }

    @SuppressWarnings("rawtypes")
    ValuesSourceAggregationBuilder with(ValuesSourceAggregationBuilder aggBuilder) {
        if (fieldName != null) {
            return aggBuilder.field(fieldName);
        }
        else {
            return aggBuilder.script(script.toPainless());
        }
    }

    public String fieldName() {
        return fieldName;
    }

    public ScriptTemplate script() {
        return script;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, script);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AggSource aggSource = (AggSource) o;
        return Objects.equals(fieldName, aggSource.fieldName) && Objects.equals(script, aggSource.script);
    }

    @Override
    public String toString() {
        return fieldName != null ? fieldName : script.toString();
    }
}
