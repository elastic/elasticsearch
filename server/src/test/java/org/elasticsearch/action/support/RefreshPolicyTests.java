/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class RefreshPolicyTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            refreshPolicy.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                WriteRequest.RefreshPolicy deserializedRefreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
                assertEquals(refreshPolicy, deserializedRefreshPolicy);
            }
        }
    }

    public void testParse() throws IOException {
        final String refreshPolicyValue = randomFrom(WriteRequest.RefreshPolicy.values()).getValue();
        assertEquals(refreshPolicyValue, WriteRequest.RefreshPolicy.parse(refreshPolicyValue).getValue());
    }

    public void testParseEmpty() throws IOException {
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, WriteRequest.RefreshPolicy.parse(""));
    }

    public void testParseUnknown() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> WriteRequest.RefreshPolicy.parse("unknown"));
        assertEquals("Unknown value for refresh: [unknown].", e.getMessage());
    }
}
