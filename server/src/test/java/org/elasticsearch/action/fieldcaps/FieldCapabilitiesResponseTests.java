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

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;

public class FieldCapabilitiesResponseTests extends ESTestCase {
    private FieldCapabilitiesResponse randomResponse() {
        Map<String, Map<String, FieldCapabilities> > fieldMap = new HashMap<> ();
        int numFields = randomInt(10);
        for (int i = 0; i < numFields; i++) {
            String fieldName = randomAlphaOfLengthBetween(5, 10);
            int numIndices = randomIntBetween(1, 5);
            Map<String, FieldCapabilities> indexFieldMap = new HashMap<> ();
            for (int j = 0; j < numIndices; j++) {
                String index = randomAlphaOfLengthBetween(10, 20);
                indexFieldMap.put(index, FieldCapabilitiesTests.randomFieldCaps());
            }
            fieldMap.put(fieldName, indexFieldMap);
        }
        return new FieldCapabilitiesResponse(fieldMap);
    }

    public void testSerialization() throws IOException {
        for (int i = 0; i < 20; i++) {
            FieldCapabilitiesResponse response = randomResponse();
            BytesStreamOutput output = new BytesStreamOutput();
            response.writeTo(output);
            output.flush();
            StreamInput input = output.bytes().streamInput();
            FieldCapabilitiesResponse deserialized = new FieldCapabilitiesResponse();
            deserialized.readFrom(input);
            assertEquals(deserialized, response);
            assertEquals(deserialized.hashCode(), response.hashCode());
        }
    }

    public void testToXContent() throws IOException {
        FieldCapabilitiesResponse response = createSimpleResponse();

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)
            .startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String generatedResponse = BytesReference.bytes(builder).utf8ToString();
        assertEquals((
            "{" +
            "    \"fields\": {" +
            "        \"rating\": { " +
            "            \"keyword\": {" +
            "                \"type\": \"keyword\"," +
            "                \"searchable\": false," +
            "                \"aggregatable\": true," +
            "                \"indices\": [\"index3\", \"index4\"]," +
            "                \"non_searchable_indices\": [\"index4\"] " +
            "            }," +
            "            \"long\": {" +
            "                \"type\": \"long\"," +
            "                \"searchable\": true," +
            "                \"aggregatable\": false," +
            "                \"indices\": [\"index1\", \"index2\"]," +
            "                \"non_aggregatable_indices\": [\"index1\"] " +
            "            }" +
            "        }," +
            "        \"title\": { " +
            "            \"text\": {" +
            "                \"type\": \"text\"," +
            "                \"searchable\": true," +
            "                \"aggregatable\": false" +
            "            }" +
            "        }" +
            "    }" +
            "}").replaceAll("\\s+", ""), generatedResponse);
    }

    private static FieldCapabilitiesResponse createSimpleResponse() {
        Map<String, FieldCapabilities> titleCapabilities = new HashMap<>();
        titleCapabilities.put("text", new FieldCapabilities("title", "text", true, false));

        Map<String, FieldCapabilities> ratingCapabilities = new HashMap<>();
        ratingCapabilities.put("long", new FieldCapabilities("rating", "long",
            true, false,
            new String[]{"index1", "index2"},
            null,
            new String[]{"index1"}));
        ratingCapabilities.put("keyword", new FieldCapabilities("rating", "keyword",
            false, true,
            new String[]{"index3", "index4"},
            new String[]{"index4"},
            null));

        Map<String, Map<String, FieldCapabilities>> responses = new HashMap<>();
        responses.put("title", titleCapabilities);
        responses.put("rating", ratingCapabilities);
        return new FieldCapabilitiesResponse(responses);
    }

    public void testFromXContent() throws IOException {
        FieldCapabilitiesResponse response = createRandomResponse();
        boolean humanReadable = randomBoolean();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference content = toShuffledXContent(response, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        Predicate<String> excludedPaths = path -> path.startsWith("fields");
        BytesReference contentWithRandomFields = insertRandomFields(xContentType, content, excludedPaths, random());

        FieldCapabilitiesResponse parsedResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), contentWithRandomFields)) {
            parsedResponse = FieldCapabilitiesResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        assertNotSame(response, parsedResponse);
        assertEquals(response, parsedResponse);
    }

    private static FieldCapabilitiesResponse createRandomResponse() {
        Map<String, Map<String, FieldCapabilities>> responses = new HashMap<>();

        String[] fields = generateRandomStringArray(5, 10, false, true);
        assertNotNull(fields);

        for (String field : fields) {
            responses.put(field, new HashMap<>());

            String[] types = generateRandomStringArray(5, 10, false, false);
            assertNotNull(types);

            for (String type : types) {
                FieldCapabilities capabilities = new FieldCapabilities(field, type,
                    randomBoolean(),
                    randomBoolean(),
                    generateRandomStringArray(5, 10, true, false),
                    generateRandomStringArray(3, 10, true, false),
                    generateRandomStringArray(3, 10, true, false));

                Map<String, FieldCapabilities> typesToCapabilities = responses.get(field);
                typesToCapabilities.put(type, capabilities);
            }
        }
        return new FieldCapabilitiesResponse(responses);
    }
}
