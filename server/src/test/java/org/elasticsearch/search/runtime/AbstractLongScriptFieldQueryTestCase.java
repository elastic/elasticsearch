/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.AbstractLongFieldScript;

import java.util.function.Function;

public abstract class AbstractLongScriptFieldQueryTestCase<T extends AbstractLongScriptFieldQuery> extends AbstractScriptFieldQueryTestCase<
    T> {
    protected final Function<LeafReaderContext, AbstractLongFieldScript> leafFactory = ctx -> null;

    @Override
    public final void testVisit() {
        assertEmptyVisit();
    }
}
