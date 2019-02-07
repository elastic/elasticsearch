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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class XContentFactoryTests extends ESTestCase {
    public void testGuessJson() throws IOException {
        testGuessType(XContentType.JSON);
    }

    public void testGuessSmile() throws IOException {
        testGuessType(XContentType.SMILE);
    }

    public void testGuessYaml() throws IOException {
        testGuessType(XContentType.YAML);
    }

    public void testGuessCbor() throws IOException {
        testGuessType(XContentType.CBOR);
    }

    private void testGuessType(XContentType type) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(type);
        builder.startObject();
        builder.field("field1", "value1");
        builder.endObject();

        final BytesReference bytes;
        if (type == XContentType.JSON && randomBoolean()) {
            final int length = randomIntBetween(0, 8 * XContentFactory.GUESS_HEADER_LENGTH);
            final String content = Strings.toString(builder);
            final StringBuilder sb = new StringBuilder(length + content.length());
            final char[] chars = new char[length];
            Arrays.fill(chars, ' ');
            sb.append(new String(chars)).append(content);
            bytes = new BytesArray(sb.toString());
        } else {
            bytes = BytesReference.bytes(builder);
        }

        assertThat(XContentHelper.xContentType(bytes), equalTo(type));
        assertThat(XContentFactory.xContentType(bytes.streamInput()), equalTo(type));

        // CBOR is binary, cannot use String
        if (type != XContentType.CBOR && type != XContentType.SMILE) {
            assertThat(XContentFactory.xContentType(Strings.toString(builder)), equalTo(type));
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
        assertThat(((Number) XContentHelper.convertToMap(new BytesArray(bytes), true).v2().get("foo")).intValue(), equalTo(5));

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

    public void testInvalidStream() throws Exception {
        byte[] bytes = new byte[] { (byte) '"' };
        assertNull(XContentFactory.xContentType(bytes));

        bytes = new byte[] { (byte) 'x' };
        assertNull(XContentFactory.xContentType(bytes));
    }

    public void testJsonFromBytesOptionallyPrecededByUtf8Bom() throws Exception {
        byte[] bytes = new byte[] {(byte) '{', (byte) '}'};
        assertThat(XContentFactory.xContentType(bytes), equalTo(XContentType.JSON));

        bytes = new byte[] {(byte) 0x20, (byte) '{', (byte) '}'};
        assertThat(XContentFactory.xContentType(bytes), equalTo(XContentType.JSON));

        bytes = new byte[] {(byte) 0xef, (byte) 0xbb, (byte) 0xbf, (byte) '{', (byte) '}'};
        assertThat(XContentFactory.xContentType(bytes), equalTo(XContentType.JSON));

        bytes = new byte[] {(byte) 0xef, (byte) 0xbb, (byte) 0xbf, (byte) 0x20, (byte) '{', (byte) '}'};
        assertThat(XContentFactory.xContentType(bytes), equalTo(XContentType.JSON));
    }
}
