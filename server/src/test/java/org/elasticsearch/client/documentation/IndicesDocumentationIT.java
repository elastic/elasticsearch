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

package org.elasticsearch.client.documentation;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.instanceOf;

/**
 * This class is used to generate the Java indices administration documentation.
 * You need to wrap your code between two tags like:
 * // tag::example[]
 * // end::example[]
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags
 * with ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{client-tests}/IndicesDocumentationIT.java[your-example-tag-here]
 * --------------------------------------------------
 */
public class IndicesDocumentationIT extends ESIntegTestCase {

    /**
     * This test method is used to generate the Put Mapping Java Indices API documentation
     * at "docs/java-api/admin/indices/put-mapping.asciidoc" so the documentation gets tested
     * so that it compiles and runs without throwing errors at runtime.
     */
     public void testPutMappingDocumentation() throws Exception {
        Client client = client();

        // tag::index-with-mapping
        client.admin().indices().prepareCreate("twitter")    // <1>
                .addMapping("tweet", "message", "type=text") // <2>
                .get();
        // end::index-with-mapping
        GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings("twitter").get();
        assertEquals(1, getMappingsResponse.getMappings().size());
        ImmutableOpenMap<String, MappingMetaData> indexMapping = getMappingsResponse.getMappings().get("twitter");
        assertThat(indexMapping.get("tweet"), instanceOf(MappingMetaData.class));

        // we need to delete in order to create a fresh new index with another type
        client.admin().indices().prepareDelete("twitter").get();
        client.admin().indices().prepareCreate("twitter").get();

        // tag::putMapping-request-source
        client.admin().indices().preparePutMapping("twitter")   // <1>
        .setType("user")                                        // <2>
        .setSource("{\n" +                                      // <3>
                "  \"properties\": {\n" +
                "    \"name\": {\n" +
                "      \"type\": \"text\"\n" +
                "    }\n" +
                "  }\n" +
                "}", XContentType.JSON)
        .get();

        // You can also provide the type in the source document
        client.admin().indices().preparePutMapping("twitter")
        .setType("user")
        .setSource("{\n" +
                "    \"user\":{\n" +                            // <4>
                "        \"properties\": {\n" +
                "            \"name\": {\n" +
                "                \"type\": \"text\"\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}", XContentType.JSON)
        .get();
        // end::putMapping-request-source
        getMappingsResponse = client.admin().indices().prepareGetMappings("twitter").get();
        assertEquals(1, getMappingsResponse.getMappings().size());
        indexMapping = getMappingsResponse.getMappings().get("twitter");
        assertEquals(singletonMap("properties", singletonMap("name", singletonMap("type", "text"))),
                indexMapping.get("user").getSourceAsMap());

        // tag::putMapping-request-source-append
        client.admin().indices().preparePutMapping("twitter")   // <1>
        .setType("user")                                        // <2>
        .setSource("{\n" +                                      // <3>
                "  \"properties\": {\n" +
                "    \"user_name\": {\n" +
                "      \"type\": \"text\"\n" +
                "    }\n" +
                "  }\n" +
                "}", XContentType.JSON)
        .get();
        // end::putMapping-request-source-append
        getMappingsResponse = client.admin().indices().prepareGetMappings("twitter").get();
        assertEquals(1, getMappingsResponse.getMappings().size());
        indexMapping = getMappingsResponse.getMappings().get("twitter");
        Map<String, Map<?,?>> expected = new HashMap<>();
        expected.put("name", singletonMap("type", "text"));
        expected.put("user_name", singletonMap("type", "text"));
        assertEquals(expected, indexMapping.get("user").getSourceAsMap().get("properties"));
    }

}
