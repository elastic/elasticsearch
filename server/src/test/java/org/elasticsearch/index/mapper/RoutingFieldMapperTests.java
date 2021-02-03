/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RoutingFieldMapperTests extends ESSingleNodeTestCase {

    public void testRoutingMapper() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject());
        DocumentMapper docMapper = createIndex("test").mapperService().parse("type", new CompressedXContent(mapping), false);

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "type", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()),
            XContentType.JSON, "routing_value"));

        assertThat(doc.rootDoc().get("_routing"), equalTo("routing_value"));
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIncludeInObjectNotAllowed() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        DocumentMapper docMapper = createIndex("test").mapperService().parse("type", new CompressedXContent(mapping), false);

        try {
            docMapper.parse(new SourceToParse("test", "type", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("_routing", "foo").endObject()),XContentType.JSON));
            fail("Expected failure to parse metadata field");
        } catch (MapperParsingException e) {
            assertThat(e.getCause().getMessage(), e.getCause().getMessage(),
                containsString("Field [_routing] is a metadata field and cannot be added inside a document"));
        }
    }
}
