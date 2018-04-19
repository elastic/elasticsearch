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

package org.elasticsearch.client;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;
import org.junit.Ignore;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;

public class FieldCapabilitiesIT extends ESRestHighLevelClientTestCase {
    @Before
    public void indexDocuments() throws Exception {
        StringEntity document1 = new StringEntity(
            "{" +
                "\"rating\": 7," +
                "\"title\": \"first title\"" +
                "}",
            ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/index1/doc/1", Collections.emptyMap(), document1);

        StringEntity mappings = new StringEntity(
            "{" +
            "  \"mappings\": {" +
            "    \"doc\": {" +
            "      \"properties\": {" +
            "        \"rating\": {" +
            "          \"type\":  \"keyword\"" +
            "        }" +
            "      }" +
            "    }" +
            "  }" +
            "}}",
            ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/" + "index2", Collections.emptyMap(), mappings);

        StringEntity document2 = new StringEntity(
            "{" +
                "\"rating\": \"good\"," +
                "\"title\": \"second title\"" +
                "}",
            ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/index2/doc/1", Collections.emptyMap(), document2);

        client().performRequest("POST", "/_refresh");
    }

    public void testBasicRequest() throws IOException {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest()
            .indices("index1", "index2")
            .fields("rating", "title");

        FieldCapabilitiesResponse response = execute(request,
            highLevelClient()::fieldCaps, highLevelClient()::fieldCapsAsync);

        // Check the capabilities for the 'rating' field.
        assertTrue(response.get().containsKey("rating"));
        Map<String, FieldCapabilities> ratingResponse = response.getField("rating");
        assertEquals(2, ratingResponse.size());

        FieldCapabilities expectedKeywordCapabilities = new FieldCapabilities(
            "rating", "keyword", true, true, new String[]{"index2"}, null, null);
        assertEquals(expectedKeywordCapabilities, ratingResponse.get("keyword"));

        FieldCapabilities expectedLongCapabilities = new FieldCapabilities(
            "rating", "long", true, true, new String[]{"index1"}, null, null);
        assertEquals(expectedLongCapabilities, ratingResponse.get("long"));

        // Check the capabilities for the 'title' field.
        assertTrue(response.get().containsKey("title"));
        Map<String, FieldCapabilities> titleResponse = response.getField("title");
        assertEquals(1, titleResponse.size());

        FieldCapabilities expectedTextCapabilities = new FieldCapabilities(
            "title", "text", true, false);
        assertEquals(expectedTextCapabilities, titleResponse.get("text"));
    }

    public void testNonexistentFields() throws IOException {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest()
            .indices("index2")
            .fields("nonexistent");

        FieldCapabilitiesResponse response = execute(request,
            highLevelClient()::fieldCaps, highLevelClient()::fieldCapsAsync);
        assertTrue(response.get().isEmpty());
    }

    public void testNoFields() {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest()
            .indices("index2");

        expectThrows(IllegalArgumentException.class,
            () -> execute(request, highLevelClient()::fieldCaps, highLevelClient()::fieldCapsAsync));
    }

    public void testNonexistentIndices() {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest()
            .indices("nonexistent")
            .fields("rating");

        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> execute(request, highLevelClient()::fieldCaps, highLevelClient()::fieldCapsAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }
}
