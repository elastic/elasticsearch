/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.Script;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public class DoubleScriptFieldTermsQuery extends AbstractDoubleScriptFieldQuery {
    private final Set<Long> terms;

    /**
     * Build the query.
     * @param terms The terms converted to a long with {@link Double#doubleToLongBits(double)}.
     */
    public DoubleScriptFieldTermsQuery(Script script, DoubleFieldScript.LeafFactory leafFactory, String fieldName, Set<Long> terms) {
        super(script, leafFactory, fieldName);
        this.terms = terms;
    }

    @Override
    protected boolean matches(double[] values, int count) {
        for (int i = 0; i < count; i++) {
            if (terms.contains(Double.doubleToLongBits(values[i]))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final String toString(String field) {
        double[] termsArray = terms();
        Arrays.sort(termsArray);
        if (fieldName().equals(field)) {
            return Arrays.toString(termsArray);
        }
        return fieldName() + ":" + Arrays.toString(termsArray);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), terms);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        DoubleScriptFieldTermsQuery other = (DoubleScriptFieldTermsQuery) obj;
        return terms.equals(other.terms);
    }

    double[] terms() {
        return terms.stream().mapToDouble(Double::longBitsToDouble).toArray();
    }
}
