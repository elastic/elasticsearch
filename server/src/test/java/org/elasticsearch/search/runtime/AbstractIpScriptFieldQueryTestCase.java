/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.IpFieldScript;

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
