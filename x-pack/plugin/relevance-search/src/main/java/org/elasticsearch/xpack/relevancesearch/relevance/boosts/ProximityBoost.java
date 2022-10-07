/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance.boosts;

import java.util.Objects;

public class ProximityBoost extends ScriptScoreBoost {
    private final String center;
    private final String function;
    private final Float factor;

    public static final String TYPE = "proximity";

    public ProximityBoost(String center, String function, Float factor) {
        super(TYPE, null);
        this.center = center;
        this.function = function;
        this.factor = factor;
    }

    public String getCenter() {
        return center;
    }

    public String getFunction() {
        return function;
    }

    public Float getFactor() {
        return factor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProximityBoost that = (ProximityBoost) o;
        return (this.function.equals(that.getFunction()) && this.center.equals(that.getCenter()) && this.factor.equals(that.getFactor()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, center, function, factor);
    }

    @Override
    public String getSource(String field) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
