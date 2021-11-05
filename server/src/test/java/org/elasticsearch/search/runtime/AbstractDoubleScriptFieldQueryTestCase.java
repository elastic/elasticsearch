/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.elasticsearch.script.DoubleFieldScript;

import static org.mockito.Mockito.mock;

public abstract class AbstractDoubleScriptFieldQueryTestCase<T extends AbstractDoubleScriptFieldQuery> extends
    AbstractScriptFieldQueryTestCase<T> {

    protected final DoubleFieldScript.LeafFactory leafFactory = mock(DoubleFieldScript.LeafFactory.class);

    @Override
    public final void testVisit() {
        assertEmptyVisit();
    }
}
