/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.versionfield.Version;

public class ValueConversionsTests extends ESTestCase {

    public void testIpConversion() throws Exception {
        {
            // ipv6 address
            BytesRef bytes = StringUtils.parseIP("2a00:1450:4007:818::200e");
            assertArrayEquals(
                new byte[] { 0x2a, 0x00, 0x14, 0x50, 0x40, 0x07, 0x08, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x0e },
                bytes.bytes
            );

            BytesRef scratch = new BytesRef();
            BytesRef bytes2 = ValueConversions.shortenIpV4Addresses(bytes.clone(), scratch);
            assertEquals(bytes, bytes2);
        }
        {
            // ipv6 mapped ipv4 address
            BytesRef bytes = StringUtils.parseIP("216.58.214.174");
            assertArrayEquals(
                new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xFF, (byte) 0xFF, (byte) 216, (byte) 58, (byte) 214, (byte) 174 },
                bytes.bytes
            );

            BytesRef scratch = new BytesRef();
            BytesRef bytes2 = ValueConversions.shortenIpV4Addresses(bytes.clone(), scratch);

            assertTrue(new BytesRef(new byte[] { (byte) 216, (byte) 58, (byte) 214, (byte) 174 }).bytesEquals(bytes2));

        }
    }

    public void testVersionConversion() {
        String version = "1.2.3-alpha";

        BytesRef bytes = new Version("1.2.3-alpha").toBytesRef();

        BytesRef scratch = new BytesRef();
        BytesRef bytes2 = ValueConversions.versionToString(bytes, scratch);

        // Some conversion happened
        assertNotEquals(bytes.length, bytes2.length);
        assertEquals(version, bytes2.utf8ToString());
    }

    public void testSourceToJson() throws Exception {
        BytesRef bytes = new BytesRef("{\"foo\": 42}");

        BytesRef scratch = new BytesRef();
        BytesRef bytes2 = ValueConversions.sourceToJson(bytes, scratch);
        // No change, even indentation
        assertEquals("{\"foo\": 42}", bytes2.utf8ToString());
    }

    public void testCborSourceToJson() throws Exception {
        XContentBuilder builder = XContentFactory.cborBuilder();
        builder.startObject();
        builder.field("foo", 42);
        builder.endObject();
        builder.close();
        BytesRef bytesRef = BytesReference.bytes(builder).toBytesRef();

        BytesRef scratch = new BytesRef();
        BytesRef bytes2 = ValueConversions.sourceToJson(bytesRef, scratch);
        // Converted to JSON
        assertEquals("{\"foo\":42}", bytes2.utf8ToString());
    }
}
