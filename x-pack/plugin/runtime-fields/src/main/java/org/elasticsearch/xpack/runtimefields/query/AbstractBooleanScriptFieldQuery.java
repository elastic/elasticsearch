/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.BooleanScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.DoubleScriptFieldScript;

/**
 * Abstract base class for building queries based on {@link DoubleScriptFieldScript}.
 */
abstract class AbstractBooleanScriptFieldQuery extends AbstractScriptFieldQuery<BooleanScriptFieldScript> {

    AbstractBooleanScriptFieldQuery(Script script, BooleanScriptFieldScript.LeafFactory leafFactory, String fieldName) {
        super(script, fieldName, leafFactory::newInstance);
    }

    @Override
    protected boolean matches(BooleanScriptFieldScript scriptContext, int docId) {
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
