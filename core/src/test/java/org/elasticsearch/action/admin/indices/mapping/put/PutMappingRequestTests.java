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

package org.elasticsearch.action.admin.indices.mapping.put;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class PutMappingRequestTests extends ESTestCase {

    public void testValidation() {
        PutMappingRequest r = new PutMappingRequest("myindex");
        ActionRequestValidationException ex = r.validate();
        assertNotNull("type validation should fail", ex);
        assertTrue(ex.getMessage().contains("type is missing"));

        r.type("");
        ex = r.validate();
        assertNotNull("type validation should fail", ex);
        assertTrue(ex.getMessage().contains("type is empty"));

        r.type("mytype");
        ex = r.validate();
        assertNotNull("source validation should fail", ex);
        assertTrue(ex.getMessage().contains("source is missing"));

        r.source("", XContentType.JSON);
        ex = r.validate();
        assertNotNull("source validation should fail", ex);
        assertTrue(ex.getMessage().contains("source is empty"));

        r.source("somevalidmapping", XContentType.JSON);
        ex = r.validate();
        assertNull("validation should succeed", ex);

        r.setConcreteIndex(new Index("foo", "bar"));
        ex = r.validate();
        assertNotNull("source validation should fail", ex);
        assertEquals(ex.getMessage(),
            "Validation Failed: 1: either concrete index or unresolved indices can be set," +
                " concrete index: [[foo/bar]] and indices: [myindex];");
    }

    public void testBuildFromSimplifiedDef() {
        // test that method rejects input where input varargs fieldname/properites are not paired correctly
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> PutMappingRequest.buildFromSimplifiedDef("type", "only_field"));
        assertEquals("mapping source must be pairs of fieldnames and properties definition.", e.getMessage());
    }

    public void testPutMappingRequestFromOldVersion() throws IOException {
        // this is hacky but here goes
        PutMappingRequest request = new PutMappingRequest("foo");
        String mapping = YamlXContent.contentBuilder().startObject().field("foo", "bar").endObject().string();
        request.source(mapping, XContentType.JSON); // THIS IS NOT A BUG! Intentionally specifying the wrong type so we serialize it
        assertEquals(mapping, request.source());

        // output version doesn't matter
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        request.writeTo(bytesStreamOutput);

        StreamInput in = StreamInput.wrap(bytesStreamOutput.bytes().toBytesRef().bytes);
        in.setVersion(Version.V_5_0_0);
        PutMappingRequest serialized = new PutMappingRequest();
        serialized.readFrom(in);

        // yaml is translated to JSON on reading
        String source = serialized.source();
        assertNotEquals(mapping, source);
        assertTrue(source.startsWith("{"));

        // reading from a fixed version does no translation
        in = StreamInput.wrap(bytesStreamOutput.bytes().toBytesRef().bytes);
        assertEquals(Version.CURRENT, in.getVersion());
        serialized = new PutMappingRequest();
        serialized.readFrom(in);
        assertEquals(mapping, serialized.source());
    }

    public void testPutMappingRequestTranslatesNonJsonToJson() throws IOException {
        // this is hacky but here goes
        PutMappingRequest request = new PutMappingRequest("foo");
        String mapping = YamlXContent.contentBuilder().startObject().field("foo", "bar").endObject().string();
        request.source(mapping, XContentType.YAML);
        assertNotEquals(mapping, request.source());
        assertTrue(request.source().startsWith("{"));
    }
}
