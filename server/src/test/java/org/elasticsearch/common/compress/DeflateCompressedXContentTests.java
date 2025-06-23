/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.compress;

import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Assert;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
        byte[] b = "---\nf:abcdefghijabcdefghij".getBytes(StandardCharsets.UTF_8);
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
        CompressedXContent compressedXContent = new CompressedXContent(toXContentObject);
        assertEquals("{}", compressedXContent.string());
    }

    public void testToXContentFragment() throws IOException {
        ToXContentFragment toXContentFragment = (builder, params) -> builder.field("field", "value");
        CompressedXContent compressedXContent = new CompressedXContent(toXContentFragment);
        assertEquals("{\"field\":\"value\"}", compressedXContent.string());
    }

    public void testEquals() throws IOException {
        final String[] randomJSON = generateRandomStringArray(1000, randomIntBetween(1, 512), false, true);
        assertNotNull(randomJSON);
        final BytesReference jsonDirect = BytesReference.bytes(
            XContentFactory.jsonBuilder().startObject().stringListField("arr", Arrays.asList(randomJSON)).endObject()
        );
        final CompressedXContent one = new CompressedXContent(jsonDirect);
        final CompressedXContent sameAsOne = new CompressedXContent(
            (builder, params) -> builder.stringListField("arr", Arrays.asList(randomJSON))
        );
        assertFalse(Arrays.equals(one.compressed(), sameAsOne.compressed()));
        assertEquals(one, sameAsOne);
    }

    public void testEqualsWhenUncompressed() throws IOException {
        final String[] randomJSON1 = generateRandomStringArray(randomIntBetween(1, 1000), randomIntBetween(1, 512), false, false);
        final String[] randomJSON2 = randomValueOtherThanMany(
            arr -> Arrays.equals(arr, randomJSON1),
            () -> generateRandomStringArray(randomIntBetween(1, 1000), randomIntBetween(1, 512), false, true)
        );
        final CompressedXContent one = new CompressedXContent(
            (builder, params) -> builder.stringListField("arr", Arrays.asList(randomJSON1))
        );
        final CompressedXContent two = new CompressedXContent(
            (builder, params) -> builder.stringListField("arr", Arrays.asList(randomJSON2))
        );
        assertNotEquals(one.uncompressed(), two.uncompressed());
    }
}
