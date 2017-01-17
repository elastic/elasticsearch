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

package org.elasticsearch.search.profile;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProfileResultTests extends ESTestCase {

    public void testToXContent() throws IOException {
        List<ProfileResult> children = new ArrayList<>();
        children.add(new ProfileResult("child1", "desc1", Collections.emptyMap(), Collections.emptyList(), 100L));
        children.add(new ProfileResult("child2", "desc2", Collections.emptyMap(), Collections.emptyList(), 123356L));
        Map<String, Long> timings = new HashMap<>();
        timings.put("key1", 12345L);
        timings.put("key2", 6789L);
        ProfileResult result = new ProfileResult("someType", "some description", timings, children, 123456L);
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"someType\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time_in_nanos\" : 123456,\n" +
                "  \"breakdown\" : {\n" +
                "    \"key1\" : 12345,\n" +
                "    \"key2\" : 6789\n" +
                "  },\n" +
                "  \"children\" : [\n" +
                "    {\n" +
                "      \"type\" : \"child1\",\n" +
                "      \"description\" : \"desc1\",\n" +
                "      \"time_in_nanos\" : 100,\n" +
                "      \"breakdown\" : { }\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\" : \"child2\",\n" +
                "      \"description\" : \"desc2\",\n" +
                "      \"time_in_nanos\" : 123356,\n" +
                "      \"breakdown\" : { }\n" +
                "    }\n" +
                "  ]\n" +
          "}", builder.string());

        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"someType\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time\" : \"123.4micros\",\n" +
                "  \"time_in_nanos\" : 123456,\n" +
                "  \"breakdown\" : {\n" +
                "    \"key1\" : 12345,\n" +
                "    \"key2\" : 6789\n" +
                "  },\n" +
                "  \"children\" : [\n" +
                "    {\n" +
                "      \"type\" : \"child1\",\n" +
                "      \"description\" : \"desc1\",\n" +
                "      \"time\" : \"100nanos\",\n" +
                "      \"time_in_nanos\" : 100,\n" +
                "      \"breakdown\" : { }\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\" : \"child2\",\n" +
                "      \"description\" : \"desc2\",\n" +
                "      \"time\" : \"123.3micros\",\n" +
                "      \"time_in_nanos\" : 123356,\n" +
                "      \"breakdown\" : { }\n" +
                "    }\n" +
                "  ]\n" +
          "}", builder.string());

        result = new ProfileResult("profileName", "some description", Collections.emptyMap(), Collections.emptyList(), 12345678L);
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"profileName\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time\" : \"12.3ms\",\n" +
                "  \"time_in_nanos\" : 12345678,\n" +
                "  \"breakdown\" : { }\n" +
              "}", builder.string());

        result = new ProfileResult("profileName", "some description", Collections.emptyMap(), Collections.emptyList(), 1234567890L);
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"profileName\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time\" : \"1.2s\",\n" +
                "  \"time_in_nanos\" : 1234567890,\n" +
                "  \"breakdown\" : { }\n" +
              "}", builder.string());
    }
}
