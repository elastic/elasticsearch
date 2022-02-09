/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.script.Script;

import java.util.Objects;

public class BooleanScriptFieldTermQuery extends AbstractBooleanScriptFieldQuery {
    private final boolean term;

    public BooleanScriptFieldTermQuery(Script script, BooleanFieldScript.LeafFactory leafFactory, String fieldName, boolean term) {
        super(script, leafFactory, fieldName);
        this.term = term;
    }

    @Override
    protected boolean matches(int trues, int falses) {
        if (term) {
            return trues > 0;
        }
        return falses > 0;
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return Boolean.toString(term);
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
        BooleanScriptFieldTermQuery other = (BooleanScriptFieldTermQuery) obj;
        return term == other.term;
    }

    boolean term() {
        return term;
    }
}
