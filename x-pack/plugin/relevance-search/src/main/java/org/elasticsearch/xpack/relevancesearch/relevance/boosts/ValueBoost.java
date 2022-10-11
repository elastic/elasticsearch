/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance.boosts;

import java.util.Objects;

public class ValueBoost extends ScriptScoreBoost {
    private final String value;
    private Float factor;

    public static final String TYPE = "value";

    public ValueBoost(String value, String operation, Float factor) {
        super(TYPE, operation);
        this.value = value;
        this.factor = factor;
    }

    public String getValue() {
        return value;
    }

    public Float getFactor() {
        return factor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueBoost that = (ValueBoost) o;
        return (this.type.equals(that.getType())
            && this.value.equals(that.getValue())
            && this.operation.equals(that.getOperation())
            && this.factor.equals(that.getFactor()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value, operation, factor);
    }

    public String getSource(String field) {
        return format(
            "(((doc[''{0}''].size() > 0) && (doc[''{0}''].value.toString() == ''{1}'')) ? {2} : {3})",
            field,
            value,
            factor,
            constantFactor()
        );
    }
}
