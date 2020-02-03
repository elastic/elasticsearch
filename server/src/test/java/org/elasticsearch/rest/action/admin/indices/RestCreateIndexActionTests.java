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

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

public class RestCreateIndexActionTests extends ESTestCase {

    public void testPrepareTypelessRequest() throws IOException {
        XContentBuilder content = XContentFactory.jsonBuilder().startObject()
            .startObject("mappings")
                .startObject("properties")
                    .startObject("field1").field("type", "keyword").endObject()
                    .startObject("field2").field("type", "text").endObject()
                .endObject()
            .endObject()
            .startObject("aliases")
                .startObject("read_alias").endObject()
            .endObject()
        .endObject();

        Map<String, Object> contentAsMap = XContentHelper.convertToMap(
            BytesReference.bytes(content), true, content.contentType()).v2();
        Map<String, Object> source = RestCreateIndexAction.prepareMappings(contentAsMap);

        XContentBuilder expectedContent = XContentFactory.jsonBuilder().startObject()
            .startObject("mappings")
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("field1").field("type", "keyword").endObject()
                        .startObject("field2").field("type", "text").endObject()
                    .endObject()
                .endObject()
            .endObject()
            .startObject("aliases")
                .startObject("read_alias").endObject()
            .endObject()
        .endObject();
        Map<String, Object> expectedContentAsMap = XContentHelper.convertToMap(
            BytesReference.bytes(expectedContent), true, expectedContent.contentType()).v2();

        assertEquals(expectedContentAsMap, source);
    }

    public void testMalformedMappings() throws IOException {
        XContentBuilder content = XContentFactory.jsonBuilder().startObject()
            .field("mappings", "some string")
            .startObject("aliases")
                .startObject("read_alias").endObject()
            .endObject()
        .endObject();

        Map<String, Object> contentAsMap = XContentHelper.convertToMap(
            BytesReference.bytes(content), true, content.contentType()).v2();

        Map<String, Object> source = RestCreateIndexAction.prepareMappings(contentAsMap);
        assertEquals(contentAsMap, source);
    }
}
