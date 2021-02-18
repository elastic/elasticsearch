/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.DoubleFieldScript;

import java.util.Objects;

public class DoubleScriptFieldTermQuery extends AbstractDoubleScriptFieldQuery {
    private final double term;

    public DoubleScriptFieldTermQuery(Script script, DoubleFieldScript.LeafFactory leafFactory, String fieldName, double term) {
        super(script, leafFactory, fieldName);
        this.term = term;
    }

    @Override
    protected boolean matches(double[] values, int count) {
        for (int i = 0; i < count; i++) {
            if (term == values[i]) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return Double.toString(term);
        }
        return fieldName() + ":" + term;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), term);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        DoubleScriptFieldTermQuery other = (DoubleScriptFieldTermQuery) obj;
        return term == other.term;
    }

    double term() {
        return term;
    }
}
