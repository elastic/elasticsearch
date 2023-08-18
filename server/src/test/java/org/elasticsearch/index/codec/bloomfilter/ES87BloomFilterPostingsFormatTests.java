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
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.codec.bloomfilter.ES87BloomFilterPostingsFormat.hashTerm;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ThreadLeakFilters(filters = { GraalVMThreadsFilter.class })
public class ES87BloomFilterPostingsFormatTests extends BasePostingsFormatTestCase {

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysPostingsFormat(new ES87BloomFilterPostingsFormat(BigArrays.NON_RECYCLING_INSTANCE, field -> {
            PostingsFormat postingsFormat = TestUtil.getDefaultPostingsFormat();
            if (postingsFormat instanceof PerFieldPostingsFormat) {
                postingsFormat = TestUtil.getDefaultPostingsFormat();
            }
            return postingsFormat;
        }));
    }

    public void testBloomFilterSize() {
        assertThat(ES87BloomFilterPostingsFormat.bloomFilterSize(1000), equalTo(10_000));
        assertThat(ES87BloomFilterPostingsFormat.bloomFilterSize(999), equalTo(9992)); // rounded to next multiple of 8
        assertThat(ES87BloomFilterPostingsFormat.bloomFilterSize(1001), equalTo(10_016)); // rounded to next multiple of 8
        assertThat(ES87BloomFilterPostingsFormat.bloomFilterSize(IndexWriter.MAX_DOCS - random().nextInt(10)), equalTo(Integer.MAX_VALUE));
        assertThat(ES87BloomFilterPostingsFormat.numBytesForBloomFilter(16384), equalTo(2048));
        assertThat(ES87BloomFilterPostingsFormat.numBytesForBloomFilter(16383), equalTo(2048));
        assertThat(ES87BloomFilterPostingsFormat.numBytesForBloomFilter(Integer.MAX_VALUE), equalTo(1 << 28));
    }

    public void testHashTermsV2() {
        // The following tests are "intentionally brittle" - the implementation of the hash function is relevant for backward-compatibility,
        // therefore these tests test the *internals* of the hash function and not only the external interface.
        Map<String, List<Integer>> testStrings = Map.of(
            "elastic",
            List.of(924743812, 1179558664, 1689188368, 51334424, 1070593832, 1580223536, 451999296),
            "bloom_filter",
            List.of(1660214456, 1257667067, 452572289, 1794961159, 184771603, 1527160473, 2064454565)
        );

        for (Map.Entry<String, List<Integer>> e : testStrings.entrySet()) {
            String term = e.getKey();
            int[] hashes = new int[7];
            final BytesRef bytesRef = randomBytesRef(term.getBytes(StandardCharsets.UTF_8));
            hashTerm(bytesRef, hashes);
            for (int i = 0; i < 7; ++i) {
                assertThat("term=" + term, hashes[i], equalTo(e.getValue().get(i)));
            }
        }

        byte[] bytes = ESTestCase.randomByteArrayOfLength(ESTestCase.between(0, 1000));
        int[] hashes = { 0, 0, 0, 0, 0, 0, 0 };
        hashTerm(randomBytesRef(bytes), hashes);
        for (int hash : hashes) {
            assertThat(hash, greaterThanOrEqualTo(0));
        }
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
