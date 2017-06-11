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

package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Base64;


public class AnalyzeRequestTests extends ESTestCase {

    public void testValidation() throws Exception {
        AnalyzeRequest request = new AnalyzeRequest();

        ActionRequestValidationException e = request.validate();
        assertNotNull("text validation should fail", e);
        assertTrue(e.getMessage().contains("text is missing"));

        request.text(new String[0]);
        e = request.validate();
        assertNotNull("text validation should fail", e);
        assertTrue(e.getMessage().contains("text is missing"));

        request.text("");
        request.normalizer("some normalizer");
        e = request.validate();
        assertNotNull("normalizer validation should fail", e);
        assertTrue(e.getMessage().contains("index is required if normalizer is specified"));

        request.index("");
        e = request.validate();
        assertNotNull("normalizer validation should fail", e);
        assertTrue(e.getMessage().contains("index is required if normalizer is specified"));

        request.index("something");
        e = request.validate();
        assertNull("something wrong in validate", e);

        request.tokenizer("tokenizer");
        e = request.validate();
        assertTrue(e.getMessage().contains("tokenizer/analyze should be null if normalizer is specified"));

        AnalyzeRequest requestAnalyzer = new AnalyzeRequest("index");
        requestAnalyzer.normalizer("some normalizer");
        requestAnalyzer.text("something");
        requestAnalyzer.analyzer("analyzer");
        e = requestAnalyzer.validate();
        assertTrue(e.getMessage().contains("tokenizer/analyze should be null if normalizer is specified"));
    }

    public void testSerialization() throws IOException {
        AnalyzeRequest request = new AnalyzeRequest("foo");
        request.text("a", "b");
        request.tokenizer("tokenizer");
        request.addTokenFilter("tokenfilter");
        request.addCharFilter("charfilter");
        request.normalizer("normalizer");

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            request.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                AnalyzeRequest serialized = new AnalyzeRequest();
                serialized.readFrom(in);
                assertArrayEquals(request.text(), serialized.text());
                assertEquals(request.tokenizer().name, serialized.tokenizer().name);
                assertEquals(request.tokenFilters().get(0).name, serialized.tokenFilters().get(0).name);
                assertEquals(request.charFilters().get(0).name, serialized.charFilters().get(0).name);
                assertEquals(request.normalizer(), serialized.normalizer());
            }
        }
    }

    public void testSerializationBwc() throws IOException {
        final byte[] data = Base64.getDecoder().decode("AAABA2ZvbwEEdGV4dAAAAAAAAAABCm5vcm1hbGl6ZXI=");
        final Version version = randomFrom(Version.V_5_0_0, Version.V_5_0_1, Version.V_5_0_2,
            Version.V_5_1_1, Version.V_5_1_2, Version.V_5_3_0, Version.V_5_3_1, Version.V_5_3_2,
            Version.V_5_4_0);
        try (StreamInput in = StreamInput.wrap(data)) {
            in.setVersion(version);
            AnalyzeRequest request = new AnalyzeRequest();
            request.readFrom(in);
            assertEquals("foo", request.index());
            assertNull("normalizer support after 6.0.0", request.normalizer());
        }
    }
}
