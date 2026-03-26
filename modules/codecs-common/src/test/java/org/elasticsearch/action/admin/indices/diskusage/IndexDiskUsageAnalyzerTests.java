/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.bloomfilter.ES94BloomFilterDocValuesFormat;

import static org.hamcrest.Matchers.notNullValue;

public class IndexDiskUsageAnalyzerTests extends AbstractIndexDiskUsageAnalyzerTestCase {

    public void testBloomFilter() throws Exception {
        final String bloomFilterField = "bloom_filter";
        // Between 32b and 64kb
        final int bloomFilterSize = 1 << randomIntBetween(5, 16);
        try (Directory dir = createNewDirectory()) {
            int numDocs = between(100, 1000);
            final Codec codec = new CodecWithBloomFilter(randomFrom(CodecMode.values()).mode(), bloomFilterSize);

            indexRandomly(dir, codec, numDocs, IndexDiskUsageAnalyzerTests::addRandomBloomFilterField);

            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            IndexDiskUsageStats.PerFieldDiskUsage bloomFieldUsage = stats.getFields().get(BloomFilterField.NAME);
            assertThat(bloomFieldUsage, notNullValue());
            assertFieldStats(
                bloomFilterField,
                "bloom_filter",
                stats.getFields().get(bloomFilterField).getBloomFilterBytes(),
                bloomFilterSize,
                0,
                0
            );
        }
    }

    public void testMixedFields() throws Exception {
        final int expectedBloomFilterSize = Math.toIntExact(ByteSizeValue.ofKb(1).getBytes());
        try (Directory dir = createNewDirectory()) {
            CodecMode codecMode = randomFrom(CodecMode.values());
            final Codec codec = new CodecWithBloomFilter(codecMode.mode(), expectedBloomFilterSize);
            indexRandomly(dir, codec, between(100, 1000), IndexDiskUsageAnalyzerTests::addRandomFields);
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            logger.info("--> stats {}", stats);
            try (Directory perFieldDir = createNewDirectory()) {
                rewriteIndexWithPerFieldCodec(dir, codecMode, perFieldDir, field -> {
                    if (field.equals(BloomFilterField.NAME)) {
                        return new ES94BloomFilterDocValuesFormat(BigArrays.NON_RECYCLING_INSTANCE, BloomFilterField.NAME);
                    }
                    return new Lucene90DocValuesFormat();
                });
                final IndexDiskUsageStats perFieldStats = collectPerFieldStats(perFieldDir);
                assertStats(stats, perFieldStats);
                assertStats(IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(perFieldDir), () -> {}), perFieldStats);
            }
        }
    }

    static class CodecWithBloomFilter extends Lucene104Codec {
        private final ES94BloomFilterDocValuesFormat bloomFilterDocValuesFormat;

        CodecWithBloomFilter(Mode mode, int bloomFilterSize) {
            super(mode);
            this.bloomFilterDocValuesFormat = new ES94BloomFilterDocValuesFormat(BigArrays.NON_RECYCLING_INSTANCE, BloomFilterField.NAME) {
                @Override
                public int bloomFilterSizeInBytesForNewSegment(int numDocs) {
                    return bloomFilterSize;
                }
            };
        }

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            if (field.equals(BloomFilterField.NAME)) {
                return bloomFilterDocValuesFormat;
            }
            return super.getDocValuesFormatForField(field);
        }
    }
}
