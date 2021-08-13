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

public class BooleanScriptFieldExistsQuery extends AbstractBooleanScriptFieldQuery {
    public BooleanScriptFieldExistsQuery(Script script, BooleanFieldScript.LeafFactory leafFactory, String fieldName) {
        super(script, leafFactory, fieldName);
    }

    @Override
    protected boolean matches(int trues, int falses) {
        return (trues | falses) != 0;
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return getClass().getSimpleName();
        }
        return fieldName() + ":" + getClass().getSimpleName();
    }

    // Superclass's equals and hashCode are great for this class
}
