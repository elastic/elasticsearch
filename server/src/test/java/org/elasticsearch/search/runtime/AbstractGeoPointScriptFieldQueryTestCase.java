/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.elasticsearch.script.GeoPointFieldScript;

import static org.mockito.Mockito.mock;

public abstract class AbstractGeoPointScriptFieldQueryTestCase<T extends AbstractGeoPointScriptFieldQuery> extends
    AbstractScriptFieldQueryTestCase<T> {

    protected final GeoPointFieldScript.LeafFactory leafFactory = mock(GeoPointFieldScript.LeafFactory.class);

    @Override
    public final void testVisit() {
        assertEmptyVisit();
    }
}
