/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.DoubleScript;

/**
 * Abstract base class for building queries based on {@link DoubleScript}.
 */
abstract class AbstractDoubleScriptFieldQuery extends AbstractScriptFieldQuery<DoubleScript> {

    AbstractDoubleScriptFieldQuery(Script script, DoubleScript.LeafFactory leafFactory, String fieldName) {
        super(script, fieldName, leafFactory::newInstance);
    }

    @Override
    protected boolean matches(DoubleScript scriptContext, int docId) {
        scriptContext.runForDoc(docId);
        return matches(scriptContext.values(), scriptContext.count());
    }

    /**
     * Does the value match this query?
     */
    protected abstract boolean matches(double[] values, int count);

    @Override
    public final void visit(QueryVisitor visitor) {
        // No subclasses contain any Terms because those have to be strings.
        if (visitor.acceptField(fieldName())) {
            visitor.visitLeaf(this);
        }
    }
}
