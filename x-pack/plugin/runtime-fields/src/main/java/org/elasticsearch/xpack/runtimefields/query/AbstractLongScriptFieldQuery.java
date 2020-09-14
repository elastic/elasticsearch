/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.AbstractLongScriptFieldScript;

import java.util.function.Function;

/**
 * Abstract base class for building queries based on {@link AbstractLongScriptFieldScript}.
 */
abstract class AbstractLongScriptFieldQuery extends AbstractScriptFieldQuery<AbstractLongScriptFieldScript> {

    AbstractLongScriptFieldQuery(
        Script script,
        Function<LeafReaderContext, AbstractLongScriptFieldScript> scriptContextFunction,
        String fieldName
    ) {
        super(script, fieldName, scriptContextFunction);
    }

    @Override
    protected boolean matches(AbstractLongScriptFieldScript scriptContext, int docId) {
        scriptContext.runForDoc(docId);
        return AbstractLongScriptFieldQuery.this.matches(scriptContext.values(), scriptContext.count());
    }

    /**
     * Does the value match this query?
     */
    protected abstract boolean matches(long[] values, int count);

    @Override
    public final void visit(QueryVisitor visitor) {
        // No subclasses contain any Terms because those have to be strings.
        if (visitor.acceptField(fieldName())) {
            visitor.visitLeaf(this);
        }
    }
}
