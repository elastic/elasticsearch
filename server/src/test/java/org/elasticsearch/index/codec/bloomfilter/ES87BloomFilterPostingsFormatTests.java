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
        assertThat(ES87BloomFilterPostingsFormat.bloomFilterSize(IndexWriter.MAX_DOCS - random().nextInt(10)), equalTo(Integer.MAX_VALUE));
        assertThat(ES87BloomFilterPostingsFormat.numBytesForBloomFilter(16384), equalTo(2048));
        assertThat(ES87BloomFilterPostingsFormat.numBytesForBloomFilter(16383), equalTo(2048));
        assertThat(ES87BloomFilterPostingsFormat.numBytesForBloomFilter(Integer.MAX_VALUE), equalTo(1 << 28));
    }

    public void testHashTermsV2() {
        // Philosophical question: In past environments, the testing philosophy was that tests should not
        // be brittle, e.g. test externally visible properties and not internal implementation details to
        // keep code agility high. These tests (hardcoding specific hash values) seem to not follow that
        // philosophy. I suspect the specificity of the tests is useful to catch compatibility issues in
        // future version upgrades?
        /*
        Map<String, List<Integer>> testStrings = Map.of(
            "hello",
            List.of(1380667438, 1968195860, 542183772, 1969437389, 1326608973, 1078019780, 359366295),
            "elasticsearch",
            List.of(2025674736, 650360721, 1781586794, 1829185816, 1226755340, 150188357, 1416753788),
            "elastic",
            List.of(1079004764, 1505351228, 1620194778, 1739313494, 2026788108, 715216665, 1356494285),
            "java",
            List.of(196343046, 1006975080, 1322262268, 1938886024, 1681507783, 531099153, 475120638),
            "lucene",
            List.of(864732739, 510526233, 750187164, 2068544193, 1512399983, 1357188193, 1553243046),
            "bloom_filter",
            List.of(302304049, 1875625365, 1390636927, 1168830145, 1298058585, 1253768250, 1824320738),
            "",
            List.of(2013023607, 407056934, 1511252831, 132787567, 1845217193, 755076846, 1422621990)
        );
        for (Map.Entry<String, List<Integer>> e : testStrings.entrySet()) {
            String term = e.getKey();
            List<Integer> hashValues = new ArrayList<>();
            for (int seed : V2_HASH_SEEDS) {
                final BytesRef bytesRef = randomBytesRef(term.getBytes(StandardCharsets.UTF_8));
                hashValues.add(hashTermWithSeed(bytesRef, seed));
            }
            assertThat("term=" + term, hashValues, equalTo(e.getValue()));
        }*/

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
