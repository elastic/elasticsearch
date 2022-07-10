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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.GraalVMThreadsFilter;

import static org.hamcrest.Matchers.equalTo;

@ThreadLeakFilters(filters = { GraalVMThreadsFilter.class })
public class BloomFilterPostingsFormatTests extends BasePostingsFormatTestCase {

    @Override
    protected Codec getCodec() {
        PostingsFormat postingsFormat = Codec.getDefault().postingsFormat();
        if (postingsFormat instanceof PerFieldPostingsFormat) {
            postingsFormat = TestUtil.getDefaultPostingsFormat();
        }
        return TestUtil.alwaysPostingsFormat(new BloomFilterPostingsFormat(postingsFormat, BigArrays.NON_RECYCLING_INSTANCE));
    }

    public void testBloomFilterSize() {
        assertThat(BloomFilterPostingsFormat.bloomFilterSize(1000), equalTo(10_000));
        assertThat(BloomFilterPostingsFormat.bloomFilterSize(IndexWriter.MAX_DOCS - random().nextInt(10)), equalTo(Integer.MAX_VALUE));
        assertThat(BloomFilterPostingsFormat.numBytesForBloomFilter(16384), equalTo(2048));
        assertThat(BloomFilterPostingsFormat.numBytesForBloomFilter(16383), equalTo(2048));
        assertThat(BloomFilterPostingsFormat.numBytesForBloomFilter(Integer.MAX_VALUE), equalTo(1 << 28));
    }
}
