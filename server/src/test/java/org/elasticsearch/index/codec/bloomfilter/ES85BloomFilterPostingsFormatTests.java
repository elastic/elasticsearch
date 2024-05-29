/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.bloomfilter;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ThreadLeakFilters(filters = { GraalVMThreadsFilter.class })
public class ES85BloomFilterPostingsFormatTests extends BasePostingsFormatTestCase {

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysPostingsFormat(new ES85BloomFilterRWPostingsFormat(BigArrays.NON_RECYCLING_INSTANCE, field -> {
            PostingsFormat postingsFormat = TestUtil.getDefaultPostingsFormat();
            if (postingsFormat instanceof PerFieldPostingsFormat) {
                postingsFormat = TestUtil.getDefaultPostingsFormat();
            }
            return postingsFormat;
        }));
    }

    public void testBloomFilterSize() {
        assertThat(ES85BloomFilterPostingsFormat.bloomFilterSize(1000), equalTo(10_000));
        assertThat(ES85BloomFilterPostingsFormat.bloomFilterSize(IndexWriter.MAX_DOCS - random().nextInt(10)), equalTo(Integer.MAX_VALUE));
        assertThat(ES85BloomFilterPostingsFormat.numBytesForBloomFilter(16384), equalTo(2048));
        assertThat(ES85BloomFilterPostingsFormat.numBytesForBloomFilter(16383), equalTo(2048));
        assertThat(ES85BloomFilterPostingsFormat.numBytesForBloomFilter(Integer.MAX_VALUE), equalTo(1 << 28));
    }

    public void testHashTerms() {
        Map<String, Integer> testStrings = Map.of(
            "hello",
            1568626408,
            "elasticsearch",
            1410942402,
            "elastic",
            255526858,
            "java",
            684588044,
            "lucene",
            881308315,
            "bloom_filter",
            83797118,
            "",
            1807139368
        );
        for (Map.Entry<String, Integer> e : testStrings.entrySet()) {
            String term = e.getKey();
            BytesRef byteRef = randomBytesRef(term.getBytes(StandardCharsets.UTF_8));
            int hash = ES85BloomFilterPostingsFormat.hashTerm(byteRef);
            assertThat("term=" + term, hash, equalTo(e.getValue()));
        }

        Map<byte[], Integer> testBytes = Map.of(
            new byte[] { 126, 49, -19, -128, 4, -77, 114, -61, 104, -58, -35, 113, 107 },
            1155258673,
            new byte[] { -50, 83, -18, 81, -44, -75, -77, 124, -76, 62, -16, 99, 75, -55, 119 },
            973344634,
            new byte[] { 110, -26, 71, -17, -113, -83, 58, 31, 13, -32, 38, -61, -97, -104, -9, -38 },
            1950254802,
            new byte[] { -20, 20, -88, 12, 5, -38, -50, 33, -21, -13, 90, 37, 28, -35, 107, 93, 30, -32, -76, 38 },
            1123005351,
            new byte[] { 88, -112, -11, -59, -103, 5, -107, -56, 14, 31, 2, -5, 67, -108, -125, 42, 28 },
            1411536425,
            new byte[] { 114, 82, -59, -103, 0, 7, -77 },
            1883229848,
            new byte[] { 34, 91, -26, 90, 21, -64, -72, 0, 101, -12, -33, 27, 119, 77, -13, 39, -60, -53 },
            603518683,
            new byte[] { 3, -68, -103, -125, 74, 122, -64, -19 },
            84707471,
            new byte[] { 0 },
            691257000,
            new byte[] { 1 },
            955192589
        );
        for (Map.Entry<byte[], Integer> e : testBytes.entrySet()) {
            byte[] term = e.getKey();
            final BytesRef bytesRef = randomBytesRef(term);
            int hash = ES85BloomFilterPostingsFormat.hashTerm(bytesRef);
            assertThat("term=" + Arrays.toString(term), hash, equalTo(e.getValue()));
        }

        byte[] bytes = ESTestCase.randomByteArrayOfLength(ESTestCase.between(0, 1000));
        assertThat(ES85BloomFilterPostingsFormat.hashTerm(randomBytesRef(bytes)), greaterThanOrEqualTo(0));
    }

    private static BytesRef randomBytesRef(byte[] bytes) {
        if (random().nextBoolean()) {
            final BytesRefBuilder builder = new BytesRefBuilder();
            // prefix
            int offset = ESTestCase.randomIntBetween(0, 10);
            builder.append(new BytesRef(ESTestCase.randomByteArrayOfLength(offset)));
            // term
            builder.append(bytes, 0, bytes.length);
            // suffix
            int suffixLength = ESTestCase.between(0, 10);
            builder.append(new BytesRef(ESTestCase.randomByteArrayOfLength(suffixLength)));
            return new BytesRef(builder.bytes(), offset, bytes.length);
        } else {
            return new BytesRef(bytes);
        }
    }
}
