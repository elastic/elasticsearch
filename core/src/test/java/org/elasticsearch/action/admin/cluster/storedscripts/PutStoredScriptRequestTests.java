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

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Base64;

public class PutStoredScriptRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        PutStoredScriptRequest storedScriptRequest =
            new PutStoredScriptRequest("foo", "bar", "context", new BytesArray("{}"), XContentType.JSON);

        assertEquals(XContentType.JSON, storedScriptRequest.xContentType());
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            storedScriptRequest.writeTo(output);

            try (StreamInput in = output.bytes().streamInput()) {
                PutStoredScriptRequest serialized = new PutStoredScriptRequest();
                serialized.readFrom(in);
                assertEquals(XContentType.JSON, serialized.xContentType());
                assertEquals(storedScriptRequest.lang(), serialized.lang());
                assertEquals(storedScriptRequest.id(), serialized.id());
                assertEquals(storedScriptRequest.context(), serialized.context());
            }
        }
    }

    public void testSerializationBwc() throws IOException {
        final byte[] rawStreamBytes = Base64.getDecoder().decode("ADwDCG11c3RhY2hlAQZzY3JpcHQCe30A");
        final Version version = randomFrom(Version.V_5_0_0, Version.V_5_0_1, Version.V_5_0_2,
            Version.V_5_1_1, Version.V_5_1_2, Version.V_5_2_0);
        try (StreamInput in = StreamInput.wrap(rawStreamBytes)) {
            in.setVersion(version);
            PutStoredScriptRequest serialized = new PutStoredScriptRequest();
            serialized.readFrom(in);
            assertEquals(XContentType.JSON, serialized.xContentType());
            assertEquals("mustache", serialized.lang());
            assertEquals("script", serialized.id());
            assertEquals(new BytesArray("{}"), serialized.content());

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                serialized.writeTo(out);
                out.flush();
                assertArrayEquals(rawStreamBytes, out.bytes().toBytesRef().bytes);
            }
        }
    }
}
