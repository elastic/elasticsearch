/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class NullValueObjectMappingTests extends MapperServiceTestCase {

    public void testNullValueObject() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {
            b.startObject("obj1");
            b.field("type", "object");
            b.endObject();
        }));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("obj1").endObject()
                        .field("value1", "test1")
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("value1"), equalTo("test1"));

        doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .nullField("obj1")
                        .field("value1", "test1")
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("value1"), equalTo("test1"));

        doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("obj1").field("field", "value").endObject()
                        .field("value1", "test1")
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("obj1.field"), equalTo("value"));
        assertThat(doc.rootDoc().get("value1"), equalTo("test1"));
    }
}
