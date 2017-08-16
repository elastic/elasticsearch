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

package org.elasticsearch.action.ingest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class PutPipelineRequestTests extends ESTestCase {

    public void testSerializationWithXContent() throws IOException {
        PutPipelineRequest request = new PutPipelineRequest("1", new BytesArray("{}".getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
        assertEquals(XContentType.JSON, request.getXContentType());

        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);

        PutPipelineRequest serialized = new PutPipelineRequest();
        serialized.readFrom(in);
        assertEquals(XContentType.JSON, serialized.getXContentType());
        assertEquals("{}", serialized.getSource().utf8ToString());
    }
}
