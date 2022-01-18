/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.Script;

/**
 * Abstract base class for building queries based on {@link DoubleFieldScript}.
 */
abstract class AbstractDoubleScriptFieldQuery extends AbstractScriptFieldQuery<DoubleFieldScript> {

    AbstractDoubleScriptFieldQuery(Script script, DoubleFieldScript.LeafFactory leafFactory, String fieldName) {
        super(script, fieldName, leafFactory::newInstance);
    }

    @Override
    protected boolean matches(DoubleFieldScript scriptContext, int docId) {
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
