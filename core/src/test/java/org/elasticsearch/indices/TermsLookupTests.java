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
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class TermsLookupTests extends ESTestCase {
    public void testTermsLookup() {
        String index = randomAsciiOfLengthBetween(1, 10);
        String type = randomAsciiOfLengthBetween(1, 10);
        String id = randomAsciiOfLengthBetween(1, 10);
        String path = randomAsciiOfLengthBetween(1, 10);
        String routing = randomAsciiOfLengthBetween(1, 10);
        TermsLookup termsLookup = new TermsLookup(index, type, id, path);
        termsLookup.routing(routing);
        assertEquals(index, termsLookup.index());
        assertEquals(type, termsLookup.type());
        assertEquals(id, termsLookup.id());
        assertEquals(path, termsLookup.path());
        assertEquals(routing, termsLookup.routing());
    }

    public void testIllegalArguments() {
        String type = randomAsciiOfLength(5);
        String id = randomAsciiOfLength(5);
        String path = randomAsciiOfLength(5);
        switch (randomIntBetween(0, 2)) {
        case 0:
            type = null;
            break;
        case 1:
            id = null;
            break;
        case 2:
            path = null;
            break;
        }
        try {
            new TermsLookup(null, type, id, path);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("[terms] query lookup element requires specifying"));
        }
    }

    public void testSerialization() throws IOException {
        TermsLookup termsLookup = randomTermsLookup();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            termsLookup.writeTo(output);
            try (StreamInput in = StreamInput.wrap(output.bytes())) {
                TermsLookup deserializedLookup = new TermsLookup(in);
                assertEquals(deserializedLookup, termsLookup);
                assertEquals(deserializedLookup.hashCode(), termsLookup.hashCode());
                assertNotSame(deserializedLookup, termsLookup);
            }
        }
    }

    public static TermsLookup randomTermsLookup() {
        return new TermsLookup(
                randomBoolean() ? randomAsciiOfLength(10) : null,
                randomAsciiOfLength(10),
                randomAsciiOfLength(10),
                randomAsciiOfLength(10).replace('.', '_')
        ).routing(randomBoolean() ? randomAsciiOfLength(10) : null);
    }
}
