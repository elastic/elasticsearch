/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;

/**
 * Common base class for script field scripts that return long values.
 */
public abstract class AbstractLongScriptFieldScript extends AbstractScriptFieldScript {
    public AbstractLongScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(params, searchLookup, ctx);
    }

    public abstract long[] execute();

    /**
     * Execute the script for the provided {@code docId}.
     */
    public final long[] runForDoc(int docId) {
        setDocument(docId);
        return execute();
    }
}
