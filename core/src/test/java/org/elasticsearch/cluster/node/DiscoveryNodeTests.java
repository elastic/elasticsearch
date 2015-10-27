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

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class DiscoveryNodeTests extends ESTestCase {

    public void testToXContent() throws IOException {
        for (ToXContent.Params params : Arrays.asList(null, ToXContent.EMPTY_PARAMS, toParams("keyed", Boolean.TRUE.toString()))) {
            assertEquals("{\n" +
                    "  \"nodes\" : {\n" +
                    "    \"_dummy_id0_\" : {\n" +
                    "      \"name\" : \"_dummy_name0_\",\n" +
                    "      \"transport_address\" : \"_dummy_addr_\",\n" +
                    "      \"attributes\" : { }\n" +
                    "    },\n" +
                    "    \"_dummy_id1_\" : {\n" +
                    "      \"name\" : \"_dummy_name1_\",\n" +
                    "      \"transport_address\" : \"_dummy_addr_\",\n" +
                    "      \"attributes\" : { }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}", buildNodes(params));
        }
    }

    public void testToXContentWithKeyedSetToFalse() throws IOException {
        // With keyed=false parameter, the nodes ids are moved to a sub field "id"
        assertEquals("{\n" +
                "  \"nodes\" : [ {\n" +
                "    \"id\" : \"_dummy_id0_\",\n" +
                "    \"name\" : \"_dummy_name0_\",\n" +
                "    \"transport_address\" : \"_dummy_addr_\",\n" +
                "    \"attributes\" : { }\n" +
                "  }, {\n" +
                "    \"id\" : \"_dummy_id1_\",\n" +
                "    \"name\" : \"_dummy_name1_\",\n" +
                "    \"transport_address\" : \"_dummy_addr_\",\n" +
                "    \"attributes\" : { }\n" +
                "  } ]\n" +
                "}", buildNodes(toParams("keyed", Boolean.FALSE.toString())));
    }

    private String buildNodes(ToXContent.Params params) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint()) {
            builder.startObject();
            builder.start("nodes", params);
            for (int i = 0; i < 2; i++) {
                String name = "_dummy_name" + String.valueOf(i) + "_";
                String id = "_dummy_id" + String.valueOf(i) + "_";
                DiscoveryNode node = new DiscoveryNode(name, id, DummyTransportAddress.INSTANCE, emptyMap(), Version.CURRENT);
                node.toXContent(builder, params);
            }
            builder.end(params);
            builder.endObject();
            return builder.string().trim();
        }
    }

    private ToXContent.Params toParams(String key, String value) {
        Map<String, String> params = new HashMap<>();
        params.put(key, value);
        return new ToXContent.MapParams(params);
    }
}