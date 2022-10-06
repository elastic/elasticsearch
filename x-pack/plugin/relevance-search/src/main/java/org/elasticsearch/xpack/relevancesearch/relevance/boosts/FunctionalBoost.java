/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance.boosts;

import java.util.Objects;

public class FunctionalBoost extends ScriptScoreBoost {
    private String function;
    private String operation;
    private Float factor;

    public static final String TYPE = "functional";

    public FunctionalBoost(String function, String operation, Float factor) {
        super(TYPE);
        this.function = function;
        this.operation = operation;
        this.factor = factor;
    }

    public String getFunction() {
        return function;
    }

    public String getOperation() {
        return operation;
    }

    public Float getFactor() {
        return factor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FunctionalBoost that = (FunctionalBoost) o;
        return (this.function.equals(that.getFunction())
            && this.operation.equals(that.getOperation())
            && this.factor.equals(that.getFactor()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, function, operation, factor);
    }
}
