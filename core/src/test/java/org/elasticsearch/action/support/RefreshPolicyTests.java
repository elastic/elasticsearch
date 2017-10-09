/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
