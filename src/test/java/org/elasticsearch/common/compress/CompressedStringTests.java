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

package org.elasticsearch.common.compress;

import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 *
 */
public class CompressedStringTests extends ElasticsearchTestCase {

    @Test
    public void simpleTestsLZF() throws IOException {
        simpleTests("lzf");
    }

    private void assertEquals(CompressedString s1, CompressedString s2) {
        Assert.assertEquals(s1, s2);
        assertArrayEquals(s1.uncompressed(), s2.uncompressed());
        assertEquals(s1.hashCode(), s2.hashCode());
    }

    public void simpleTests(String compressor) throws IOException {
        CompressorFactory.configure(ImmutableSettings.settingsBuilder().put("compress.default.type", compressor).build());
        String str = "this is a simple string";
        CompressedString cstr = new CompressedString(str);
        assertThat(cstr.string(), equalTo(str));
        assertThat(new CompressedString(str), equalTo(cstr));

        String str2 = "this is a simple string 2";
        CompressedString cstr2 = new CompressedString(str2);
        assertThat(cstr2.string(), not(equalTo(str)));
        assertThat(new CompressedString(str2), not(equalTo(cstr)));
        assertEquals(new CompressedString(str2), cstr2);
    }

    public void testRandom() throws IOException {
        String compressor = "lzf";
        CompressorFactory.configure(ImmutableSettings.settingsBuilder().put("compress.default.type", compressor).build());
        Random r = getRandom();
        for (int i = 0; i < 1000; i++) {
            String string = TestUtil.randomUnicodeString(r, 10000);
            CompressedString compressedString = new CompressedString(string);
            assertThat(compressedString.string(), equalTo(string));
        }
    }

    public void testDifferentCompressedRepresentation() throws Exception {
        byte[] b = "abcdefghijabcdefghij".getBytes("UTF-8");
        CompressorFactory.defaultCompressor();

        Compressor compressor = CompressorFactory.defaultCompressor();
        BytesStreamOutput bout = new BytesStreamOutput();
        StreamOutput out = compressor.streamOutput(bout);
        out.writeBytes(b);
        out.flush();
        out.writeBytes(b);
        out.close();
        final BytesReference b1 = bout.bytes();

        bout = new BytesStreamOutput();
        out = compressor.streamOutput(bout);
        out.writeBytes(b);
        out.writeBytes(b);
        out.close();
        final BytesReference b2 = bout.bytes();

        // because of the intermediate flush, the two compressed representations
        // are different. It can also happen for other reasons like if hash tables
        // of different size are being used
        assertFalse(b1.equals(b2));
        // we used the compressed representation directly and did not recompress
        assertArrayEquals(b1.toBytes(), new CompressedString(b1).compressed());
        assertArrayEquals(b2.toBytes(), new CompressedString(b2).compressed());
        // but compressedstring instances are still equal
        assertEquals(new CompressedString(b1), new CompressedString(b2));
    }

    public void testHashCode() throws IOException {
        assertFalse(new CompressedString("a").hashCode() == new CompressedString("b").hashCode());
    }

}
