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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class MergedFieldCapabilitiesResponseTests extends AbstractSerializingTestCase<FieldCapabilitiesResponse> {

    @Override
    protected FieldCapabilitiesResponse doParseInstance(XContentParser parser) throws IOException {
        return FieldCapabilitiesResponse.fromXContent(parser);
    }

    @Override
    protected FieldCapabilitiesResponse createTestInstance() {
        // merged responses
        Map<String, Map<String, FieldCapabilities>> responses = new HashMap<>();

        String[] fields = generateRandomStringArray(5, 10, false, true);
        assertNotNull(fields);

        for (String field : fields) {
            Map<String, FieldCapabilities> typesToCapabilities = new HashMap<>();
            String[] types = generateRandomStringArray(5, 10, false, false);
            assertNotNull(types);

            for (String type : types) {
                typesToCapabilities.put(type, FieldCapabilitiesTests.randomFieldCaps(field));
            }
            responses.put(field, typesToCapabilities);
        }
        int numIndices = randomIntBetween(1, 10);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        return new FieldCapabilitiesResponse(indices, responses);
    }

    @Override
    protected Writeable.Reader<FieldCapabilitiesResponse> instanceReader() {
        return FieldCapabilitiesResponse::new;
    }

    @Override
    protected FieldCapabilitiesResponse mutateInstance(FieldCapabilitiesResponse response) {
        Map<String, Map<String, FieldCapabilities>> mutatedResponses = new HashMap<>(response.get());

        int mutation = response.get().isEmpty() ? 0 : randomIntBetween(0, 2);

        switch (mutation) {
            case 0:
                String toAdd = randomAlphaOfLength(10);
                mutatedResponses.put(toAdd, Collections.singletonMap(
                    randomAlphaOfLength(10),
                    FieldCapabilitiesTests.randomFieldCaps(toAdd)));
                break;
            case 1:
                String toRemove = randomFrom(mutatedResponses.keySet());
                mutatedResponses.remove(toRemove);
                break;
            case 2:
                String toReplace = randomFrom(mutatedResponses.keySet());
                mutatedResponses.put(toReplace, Collections.singletonMap(
                    randomAlphaOfLength(10),
                    FieldCapabilitiesTests.randomFieldCaps(toReplace)));
                break;
        }
        return new FieldCapabilitiesResponse(null, mutatedResponses);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Disallow random fields from being inserted under the 'fields' key, as this
        // map only contains field names, and also under 'fields.FIELD_NAME', as these
        // maps only contain type names.
        return field -> field.matches("fields(\\.\\w+)?");
    }

    public void testToXContent() throws IOException {
        FieldCapabilitiesResponse response = createSimpleResponse();

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String generatedResponse = BytesReference.bytes(builder).utf8ToString();
        assertEquals((
            "{" +
            "    \"indices\": null," +
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

    public void testEmptyResponse() throws IOException {
        FieldCapabilitiesResponse testInstance = new FieldCapabilitiesResponse();
        assertSerialization(testInstance);
    }

    private static FieldCapabilitiesResponse createSimpleResponse() {
        Map<String, FieldCapabilities> titleCapabilities = new HashMap<>();
        titleCapabilities.put("text", new FieldCapabilities("title", "text", true, false, Collections.emptyMap()));

        Map<String, FieldCapabilities> ratingCapabilities = new HashMap<>();
        ratingCapabilities.put("long", new FieldCapabilities("rating", "long",
            true, false,
            new String[]{"index1", "index2"},
            null,
            new String[]{"index1"}, Collections.emptyMap()));
        ratingCapabilities.put("keyword", new FieldCapabilities("rating", "keyword",
            false, true,
            new String[]{"index3", "index4"},
            new String[]{"index4"},
            null, Collections.emptyMap()));

        Map<String, Map<String, FieldCapabilities>> responses = new HashMap<>();
        responses.put("title", titleCapabilities);
        responses.put("rating", ratingCapabilities);
        return new FieldCapabilitiesResponse(null, responses);
    }
}
