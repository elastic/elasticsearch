/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.Script;

/**
 * Abstract base class for building queries based on {@link DoubleFieldScript}.
 */
abstract class AbstractBooleanScriptFieldQuery extends AbstractScriptFieldQuery<BooleanFieldScript> {

    AbstractBooleanScriptFieldQuery(Script script, BooleanFieldScript.LeafFactory leafFactory, String fieldName) {
        super(script, fieldName, leafFactory::newInstance);
    }

    @Override
    protected boolean matches(BooleanFieldScript scriptContext, int docId) {
        scriptContext.runForDoc(docId);
        return matches(scriptContext.trues(), scriptContext.falses());
    }

    /**
     * Does the value match this query?
     * @param trues the number of true values returned by the script
     * @param falses the number of false values returned by the script
     */
    protected abstract boolean matches(int trues, int falses);

    @Override
    public final void visit(QueryVisitor visitor) {
        // No subclasses contain any Terms because those have to be strings.
        if (visitor.acceptField(fieldName())) {
            visitor.visitLeaf(this);
        }
    }
}
