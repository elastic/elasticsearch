/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

public class RemoteFetchHandleTests extends ESTestCase {
    public void testBytesRefRoundTrip() {
        RemoteFetchHandle handle = randomHandle();

        BytesRef encoded = handle.toBytesRef();

        assertEquals(handle, RemoteFetchHandle.fromBytesRef(encoded));
    }

    private RemoteFetchHandle randomHandle() {
        return new RemoteFetchHandle(
            randomAlphaOfLengthBetween(5, 12),
            randomAlphaOfLengthBetween(5, 16),
            randomIntBetween(0, 1024),
            randomIntBetween(0, 4096),
            randomIntBetween(0, 1 << 20)
        );
    }
}
