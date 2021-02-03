/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.runtimefields.mapper.IpFieldScript;

import java.net.InetAddress;

import static org.mockito.Mockito.mock;

public abstract class AbstractIpScriptFieldQueryTestCase<T extends AbstractIpScriptFieldQuery> extends AbstractScriptFieldQueryTestCase<T> {

    protected final IpFieldScript.LeafFactory leafFactory = mock(IpFieldScript.LeafFactory.class);

    @Override
    public final void testVisit() {
        assertEmptyVisit();
    }

    protected static BytesRef encode(InetAddress addr) {
        return new BytesRef(InetAddressPoint.encode(addr));
    }
}
