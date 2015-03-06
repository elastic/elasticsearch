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

package org.elasticsearch.common.xcontent;

import com.fasterxml.jackson.dataformat.cbor.CBORConstants;
import com.fasterxml.jackson.dataformat.smile.SmileConstants;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class XContentFactoryTests extends ElasticsearchTestCase {


    @Test
    public void testGuessJson() throws IOException {
        testGuessType(XContentType.JSON);
    }

    @Test
    public void testGuessSmile() throws IOException {
        testGuessType(XContentType.SMILE);
    }

    @Test
    public void testGuessYaml() throws IOException {
        testGuessType(XContentType.YAML);
    }

    @Test
    public void testGuessCbor() throws IOException {
        testGuessType(XContentType.CBOR);
    }

    private void testGuessType(XContentType type) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(type);
        builder.startObject();
        builder.field("field1", "value1");
        builder.endObject();

        assertThat(XContentFactory.xContentType(builder.bytes()), equalTo(type));
        BytesArray bytesArray = builder.bytes().toBytesArray();
        assertThat(XContentFactory.xContentType(new BytesStreamInput(bytesArray.array(), bytesArray.arrayOffset(), bytesArray.length())), equalTo(type));

        // CBOR is binary, cannot use String
        if (type != XContentType.CBOR) {
            assertThat(XContentFactory.xContentType(builder.string()), equalTo(type));
        }
    }

    public void testCBORBasedOnMajorObjectDetection() {
        // for this {"f "=> 5} perl encoder for example generates:
        byte[] bytes = new byte[] {(byte) 0xA1, (byte) 0x43, (byte) 0x66, (byte) 6f, (byte) 6f, (byte) 0x5};
        assertThat(XContentFactory.xContentType(bytes), equalTo(XContentType.CBOR));
        //assertThat(((Number) XContentHelper.convertToMap(bytes, true).v2().get("foo")).intValue(), equalTo(5));

        // this if for {"foo" : 5} in python CBOR
        bytes = new byte[] {(byte) 0xA1, (byte) 0x63, (byte) 0x66, (byte) 0x6f, (byte) 0x6f, (byte) 0x5};
        assertThat(XContentFactory.xContentType(bytes), equalTo(XContentType.CBOR));
        assertThat(((Number) XContentHelper.convertToMap(bytes, true).v2().get("foo")).intValue(), equalTo(5));

        // also make sure major type check doesn't collide with SMILE and JSON, just in case
        assertThat(CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_OBJECT, SmileConstants.HEADER_BYTE_1), equalTo(false));
        assertThat(CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_OBJECT, (byte) '{'), equalTo(false));
        assertThat(CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_OBJECT, (byte) ' '), equalTo(false));
        assertThat(CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_OBJECT, (byte) '-'), equalTo(false));
    }

    public void testCBORBasedOnMagicHeaderDetection() {
        byte[] bytes = new byte[] {(byte) 0xd9, (byte) 0xd9, (byte) 0xf7};
        assertThat(XContentFactory.xContentType(bytes), equalTo(XContentType.CBOR));
    }

    public void testEmptyStream() throws Exception {
        ByteArrayInputStream is = new ByteArrayInputStream(new byte[0]);
        assertNull(XContentFactory.xContentType(is));

        is = new ByteArrayInputStream(new byte[] {(byte) 1});
        assertNull(XContentFactory.xContentType(is));
    }
}
