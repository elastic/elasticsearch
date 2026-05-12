/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.tsdb.AbstractPrefixPartitionTests;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesTestUtil;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;

public class PrefixPartitionTests extends AbstractPrefixPartitionTests {

    @Override
    protected Codec getCodec(boolean writePrefixPartitions) {
        return TestUtil.alwaysDocValuesFormat(
            new ES95TSDBDocValuesFormat(
                random().nextInt(4, 16),
                random().nextInt(1, 32),
                random().nextBoolean(),
                TSDBDocValuesTestUtil.randomBinaryCompressionMode(),
                random().nextBoolean(),
                TSDBDocValuesTestUtil.randomNumericBlockSize(),
                writePrefixPartitions,
                ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT,
                ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT,
                NumericCodecFactory.DEFAULT,
                ES95NumericFieldReader::defaultFallbackDecoder
            )
        );
    }
}
