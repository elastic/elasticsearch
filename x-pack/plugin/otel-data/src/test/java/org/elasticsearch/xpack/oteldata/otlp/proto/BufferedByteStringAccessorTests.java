/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.proto;

import com.google.protobuf.ByteString;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import static org.hamcrest.Matchers.equalTo;

public class BufferedByteStringAccessorTests extends ESTestCase {

    private final BufferedByteStringAccessor accessor = new BufferedByteStringAccessor();

    public void testAddStringDimension() {
        String value = randomUnicodeOfLengthBetween(1, 1000);
        TsidBuilder byteStringTsidBuilder = new TsidBuilder();
        accessor.addStringDimension(byteStringTsidBuilder, "test", ByteString.copyFromUtf8(value));
        TsidBuilder basicTsidBuilder = new TsidBuilder();
        basicTsidBuilder.addStringDimension("test", value);
        assertThat(byteStringTsidBuilder.hash(), equalTo(basicTsidBuilder.hash()));
    }

    public void testAddEmptyStringDimension() {
        TsidBuilder byteStringTsidBuilder = new TsidBuilder();
        accessor.addStringDimension(byteStringTsidBuilder, "test", ByteString.copyFromUtf8(""));
        assertThat(byteStringTsidBuilder.size(), equalTo(0));
    }

    public void testUtf8Value() throws Exception {
        String value = randomUnicodeOfLengthBetween(0, 1000);
        String json;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field("value");
            accessor.utf8Value(builder, ByteString.copyFromUtf8(value));
            builder.endObject();
            json = Strings.toString(builder);
        }

        assertThat(XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false).get("value"), equalTo(value));
    }

}
