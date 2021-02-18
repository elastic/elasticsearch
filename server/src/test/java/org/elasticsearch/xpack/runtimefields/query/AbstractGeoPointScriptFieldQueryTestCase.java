/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.elasticsearch.xpack.runtimefields.mapper.GeoPointFieldScript;

import static org.mockito.Mockito.mock;

public abstract class AbstractGeoPointScriptFieldQueryTestCase<T extends AbstractGeoPointScriptFieldQuery> extends
    AbstractScriptFieldQueryTestCase<T> {

    protected final GeoPointFieldScript.LeafFactory leafFactory = mock(GeoPointFieldScript.LeafFactory.class);

    @Override
    public final void testVisit() {
        assertEmptyVisit();
    }
}
