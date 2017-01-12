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

public class PutStoredScriptRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        PutStoredScriptRequest storedScriptRequest = new PutStoredScriptRequest("foo", "bar");
        storedScriptRequest.script(new BytesArray("{}"), XContentType.JSON);

        assertEquals(XContentType.JSON, storedScriptRequest.xContentType());
        BytesStreamOutput output = new BytesStreamOutput();
        storedScriptRequest.writeTo(output);

        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);
        PutStoredScriptRequest serialized = new PutStoredScriptRequest();
        serialized.readFrom(in);
        assertEquals(XContentType.JSON, storedScriptRequest.xContentType());
        assertEquals(storedScriptRequest.scriptLang(), serialized.scriptLang());
        assertEquals(storedScriptRequest.id(), serialized.id());

        // send to an old version and then read it
        output = new BytesStreamOutput();
        output.setVersion(Version.V_5_0_0);
        storedScriptRequest.writeTo(output);
        in = StreamInput.wrap(output.bytes().toBytesRef().bytes);
        in.setVersion(Version.V_5_0_0);
        serialized = new PutStoredScriptRequest();
        serialized.readFrom(in);
        assertEquals(XContentType.JSON, storedScriptRequest.xContentType());
        assertEquals(storedScriptRequest.scriptLang(), serialized.scriptLang());
        assertEquals(storedScriptRequest.id(), serialized.id());
    }
}
