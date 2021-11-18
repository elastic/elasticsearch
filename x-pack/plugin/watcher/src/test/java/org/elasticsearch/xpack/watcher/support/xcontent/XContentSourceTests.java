/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.support.xcontent;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xcontent.XContentFactory.smileBuilder;
import static org.elasticsearch.xcontent.XContentFactory.yamlBuilder;

public class XContentSourceTests extends ESTestCase {
    public void testToXContent() throws Exception {
        XContentBuilder builder = randomBoolean() ? jsonBuilder() : randomBoolean() ? yamlBuilder() : smileBuilder();
        BytesReference bytes = randomBoolean()
            ? BytesReference.bytes(builder.startObject().field("key", "value").endObject())
            : BytesReference.bytes(
                builder.startObject()
                    .field("key_str", "value")
                    .startArray("array_int")
                    .value(randomInt(10))
                    .endArray()
                    .nullField("key_null")
                    .endObject()
            );
        XContentSource source = new XContentSource(bytes, builder.contentType());
        XContentBuilder builder2 = XContentFactory.contentBuilder(builder.contentType());
        BytesReference bytes2 = BytesReference.bytes(source.toXContent(builder2, ToXContent.EMPTY_PARAMS));
        assertEquals(bytes.toBytesRef(), bytes2.toBytesRef());
    }
}
