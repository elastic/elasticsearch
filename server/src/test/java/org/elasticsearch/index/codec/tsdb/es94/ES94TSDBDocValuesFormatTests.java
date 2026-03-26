/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es94;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesFormatTestCase;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.DocOffsetsCodec;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormatTests;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.codec.tsdb.es94.ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT;
import static org.elasticsearch.index.codec.tsdb.es94.ES94TSDBDocValuesFormat.NUMERIC_LARGE_BLOCK_SHIFT;
import static org.hamcrest.Matchers.equalTo;

public class ES94TSDBDocValuesFormatTests extends AbstractTSDBDocValuesFormatTestCase {

    protected final Codec es94Codec = new Elasticsearch93Lucene104Codec() {

        final DocValuesFormat docValuesFormat = new ES94TSDBDocValuesFormat(
            ESTestCase.randomIntBetween(2, 4096),
            ESTestCase.randomIntBetween(1, 512),
            random().nextBoolean(),
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            random().nextBoolean() ? NUMERIC_LARGE_BLOCK_SHIFT : NUMERIC_BLOCK_SHIFT
        );

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return docValuesFormat;
        }
    };

    @Override
    protected Codec getCodec() {
        return es94Codec;
    }

    @Override
    protected DocValuesFormat createDocValuesFormat(
        int skipIndexIntervalSize,
        int minDocsPerOrdinalForRangeEncoding,
        boolean enableOptimizedMerge,
        BinaryDVCompressionMode binaryDVCompressionMode,
        boolean enablePerBlockCompression,
        int numericBlockShift,
        boolean writePrefixPartitions
    ) {
        return new ES94TSDBDocValuesFormat(
            skipIndexIntervalSize,
            minDocsPerOrdinalForRangeEncoding,
            enableOptimizedMerge,
            binaryDVCompressionMode,
            enablePerBlockCompression,
            numericBlockShift,
            DocOffsetsCodec.GROUPED_VINT,
            ES94TSDBDocValuesFormat.BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT,
            ES94TSDBDocValuesFormat.BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT,
            writePrefixPartitions
        );
    }

    public void testBinaryCompressionEnabled() {
        ES94TSDBDocValuesFormat docValueFormat = new ES94TSDBDocValuesFormat();
        assertThat(docValueFormat.formatConfig.binaryCompressionMode(), equalTo(BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1));
    }

    public void testAddIndices() throws IOException {
        doTestAddIndices(
            List.of(
                new ES87TSDBDocValuesFormatTests.TestES87TSDBDocValuesFormat(random().nextInt(4, 16)),
                new ES819TSDBDocValuesFormat(),
                new ES819Version3TSDBDocValuesFormat(),
                new ES94TSDBDocValuesFormat(NUMERIC_BLOCK_SHIFT),
                new ES94TSDBDocValuesFormat(NUMERIC_LARGE_BLOCK_SHIFT),
                new Lucene90DocValuesFormat()
            )
        );
    }
}
