/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

public class RestCreateIndexActionTests extends ESTestCase {

    public void testPrepareTypelessRequest() throws IOException {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .startObject("field2")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .startObject("aliases")
            .startObject("read_alias")
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> contentAsMap = XContentHelper.convertToMap(BytesReference.bytes(content), true, content.contentType()).v2();
        Map<String, Object> source = RestCreateIndexAction.prepareMappings(contentAsMap);

        XContentBuilder expectedContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("mappings")
            .startObject("_doc")
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .startObject("field2")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .startObject("aliases")
            .startObject("read_alias")
            .endObject()
            .endObject()
            .endObject();
        Map<String, Object> expectedContentAsMap = XContentHelper.convertToMap(
            BytesReference.bytes(expectedContent),
            true,
            expectedContent.contentType()
        ).v2();

        assertEquals(expectedContentAsMap, source);
    }

    public void testMalformedMappings() throws IOException {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .field("mappings", "some string")
            .startObject("aliases")
            .startObject("read_alias")
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> contentAsMap = XContentHelper.convertToMap(BytesReference.bytes(content), true, content.contentType()).v2();

        Map<String, Object> source = RestCreateIndexAction.prepareMappings(contentAsMap);
        assertEquals(contentAsMap, source);
    }
}
