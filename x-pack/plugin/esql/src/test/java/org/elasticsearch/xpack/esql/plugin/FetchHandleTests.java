/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

public class FetchHandleTests extends ESTestCase {
    public void testBytesRefRoundTrip() {
        FetchHandle handle = randomHandle();

        BytesRef encoded = handle.toBytesRef();

        assertEquals(handle, FetchHandle.fromBytesRef(encoded));
    }

    private FetchHandle randomHandle() {
        return new FetchHandle(
            randomAlphaOfLengthBetween(5, 12),
            randomAlphaOfLengthBetween(5, 16),
            randomIntBetween(0, 1024),
            randomIntBetween(0, 4096),
            randomIntBetween(0, 1 << 20)
        );
    }
}
