/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.runtime;

import org.elasticsearch.script.GeometryFieldScript;
import org.elasticsearch.search.runtime.AbstractScriptFieldQueryTestCase;

import static org.mockito.Mockito.mock;

public abstract class AbstractGeoShapeScriptFieldQueryTestCase<T extends AbstractGeoShapeScriptFieldQuery> extends
    AbstractScriptFieldQueryTestCase<T> {

    protected final GeometryFieldScript.LeafFactory leafFactory = mock(GeometryFieldScript.LeafFactory.class);

    @Override
    public final void testVisit() {
        assertEmptyVisit();
    }
}
