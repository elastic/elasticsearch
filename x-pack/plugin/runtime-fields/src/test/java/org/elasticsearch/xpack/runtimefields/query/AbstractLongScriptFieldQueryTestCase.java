/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.xpack.runtimefields.AbstractLongScriptFieldScript;

import java.io.IOException;

public abstract class AbstractLongScriptFieldQueryTestCase<T extends AbstractLongScriptFieldQuery> extends AbstractScriptFieldQueryTestCase<
    T> {
    protected final CheckedFunction<LeafReaderContext, AbstractLongScriptFieldScript, IOException> leafFactory = ctx -> null;

    @Override
    public final void testVisit() {
        assertEmptyVisit();
    }
}
