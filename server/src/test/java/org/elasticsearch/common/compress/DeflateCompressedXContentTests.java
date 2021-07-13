/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.compress;

import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class DeflateCompressedXContentTests extends ESTestCase {

    private final Compressor compressor = new DeflateCompressor();

    private void assertEquals(CompressedXContent s1, CompressedXContent s2) {
        Assert.assertEquals(s1, s2);
        assertEquals(s1.uncompressed(), s2.uncompressed());
        assertEquals(s1.hashCode(), s2.hashCode());
    }

    public void simpleTests() throws IOException {
        String str = "---\nf:this is a simple string";
        CompressedXContent cstr = new CompressedXContent(str);
        assertThat(cstr.string(), equalTo(str));
        assertThat(new CompressedXContent(str), equalTo(cstr));

        String str2 = "---\nf:this is a simple string 2";
        CompressedXContent cstr2 = new CompressedXContent(str2);
        assertThat(cstr2.string(), not(equalTo(str)));
        assertThat(new CompressedXContent(str2), not(equalTo(cstr)));
        assertEquals(new CompressedXContent(str2), cstr2);
    }

    public void testRandom() throws IOException {
        Random r = random();
        for (int i = 0; i < 1000; i++) {
            String string = TestUtil.randomUnicodeString(r, 10000);
            // hack to make it detected as YAML
            string = "---\n" + string;
            CompressedXContent compressedXContent = new CompressedXContent(string);
            assertThat(compressedXContent.string(), equalTo(string));
        }
    }

    public void testDifferentCompressedRepresentation() throws Exception {
        byte[] b = "---\nf:abcdefghijabcdefghij".getBytes("UTF-8");
        BytesStreamOutput bout = new BytesStreamOutput();
        try (OutputStream out = compressor.threadLocalOutputStream(bout)) {
            out.write(b);
            out.flush();
            out.write(b);
        }
        final BytesReference b1 = bout.bytes();

        bout = new BytesStreamOutput();
        try (OutputStream out = compressor.threadLocalOutputStream(bout)) {
            out.write(b);
            out.write(b);
        }
        final BytesReference b2 = bout.bytes();

        // because of the intermediate flush, the two compressed representations
        // are different. It can also happen for other reasons like if hash tables
        // of different size are being used
        assertFalse(b1.equals(b2));
        // we used the compressed representation directly and did not recompress
        assertArrayEquals(BytesReference.toBytes(b1), new CompressedXContent(b1).compressed());
        assertArrayEquals(BytesReference.toBytes(b2), new CompressedXContent(b2).compressed());
        // but compressedstring instances are still equal
        assertEquals(new CompressedXContent(b1), new CompressedXContent(b2));
    }

    public void testHashCode() throws IOException {
        assertFalse(new CompressedXContent("{\"a\":\"b\"}").hashCode() == new CompressedXContent("{\"a\":\"c\"}").hashCode());
    }

    public void testToXContentObject() throws IOException {
        ToXContentObject toXContentObject = (builder, params) -> {
            builder.startObject();
            builder.endObject();
            return builder;
        };
        CompressedXContent compressedXContent = new CompressedXContent(toXContentObject, XContentType.JSON, ToXContent.EMPTY_PARAMS);
        assertEquals("{}", compressedXContent.string());
    }

    public void testToXContentFragment() throws IOException {
        ToXContentFragment toXContentFragment = (builder, params) -> builder.field("field", "value");
        CompressedXContent compressedXContent = new CompressedXContent(toXContentFragment, XContentType.JSON, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"field\":\"value\"}", compressedXContent.string());
    }
}
