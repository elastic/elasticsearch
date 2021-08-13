/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ClearSecurityCacheRequestTests extends ESTestCase {

    public void testSerialisation() throws IOException {
        final String cacheName = randomAlphaOfLengthBetween(4, 8);
        final String[] keys = randomArray(0, 8, String[]::new, () -> randomAlphaOfLength(12));
        final ClearSecurityCacheRequest request = new ClearSecurityCacheRequest();
        request.cacheName(cacheName).keys(keys);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                final ClearSecurityCacheRequest serialized = new ClearSecurityCacheRequest(in);
                assertEquals(request.cacheName(), serialized.cacheName());
                assertArrayEquals(request.keys(), serialized.keys());
            }
        }
    }
}
