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

package org.elasticsearch.indices;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class TermsLookupTests extends ESTestCase {
    public void testTermsLookup() {
        String index = randomAlphaOfLengthBetween(1, 10);
        String type = randomAlphaOfLengthBetween(1, 10);
        String id = randomAlphaOfLengthBetween(1, 10);
        String path = randomAlphaOfLengthBetween(1, 10);
        String routing = randomAlphaOfLengthBetween(1, 10);
        TermsLookup termsLookup = new TermsLookup(index, type, id, path);
        termsLookup.routing(routing);
        assertEquals(index, termsLookup.index());
        assertEquals(type, termsLookup.type());
        assertEquals(id, termsLookup.id());
        assertEquals(path, termsLookup.path());
        assertEquals(routing, termsLookup.routing());
    }

    public void testIllegalArguments() {
        String type = randomAlphaOfLength(5);
        String id = randomAlphaOfLength(5);
        String path = randomAlphaOfLength(5);
        String index = randomAlphaOfLength(5);
        switch (randomIntBetween(0, 3)) {
            case 0:
                type = null;
                break;
            case 1:
                id = null;
                break;
            case 2:
                path = null;
                break;
            case 3:
                index = null;
                break;
            default:
                fail("unknown case");
        }
        try {
            new TermsLookup(index, type, id, path);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("[terms] query lookup element requires specifying"));
        }
    }

    public void testSerialization() throws IOException {
        TermsLookup termsLookup = randomTermsLookup();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            termsLookup.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                TermsLookup deserializedLookup = new TermsLookup(in);
                assertEquals(deserializedLookup, termsLookup);
                assertEquals(deserializedLookup.hashCode(), termsLookup.hashCode());
                assertNotSame(deserializedLookup, termsLookup);
            }
        }
    }

    public static TermsLookup randomTermsLookup() {
        return new TermsLookup(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10).replace('.', '_')
        ).routing(randomBoolean() ? randomAlphaOfLength(10) : null);
    }
}
