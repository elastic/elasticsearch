/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.xcontent;


import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.smileBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.yamlBuilder;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class XContentSourceTests extends ESTestCase {
    public void testToXContent() throws Exception {
        XContentBuilder builder = randomBoolean() ? jsonBuilder() : randomBoolean() ? yamlBuilder() : smileBuilder();
        BytesReference bytes = randomBoolean() ?
                builder.startObject().field("key", "value").endObject().bytes() :
                builder.startObject()
                        .field("key_str", "value")
                        .startArray("array_int").value(randomInt(10)).endArray()
                        .nullField("key_null")
                        .endObject()
                        .bytes();
        XContentSource source = new XContentSource(bytes, builder.contentType());
        XContentBuilder builder2 = XContentFactory.contentBuilder(builder.contentType());
        BytesReference bytes2 = source.toXContent(builder2, ToXContent.EMPTY_PARAMS).bytes();
        assertThat(bytes.array(), is(bytes2.array()));
    }
}
