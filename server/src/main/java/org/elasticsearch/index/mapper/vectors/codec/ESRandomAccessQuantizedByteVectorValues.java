/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors.codec;

import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

import java.io.IOException;

/**
 * Random access values for <code>byte[]</code>, but also includes accessing the score correction
 * constant for the current vector in the buffer.
 */
interface ESRandomAccessQuantizedByteVectorValues extends RandomAccessVectorValues<byte[]> {
    float getScoreCorrectionConstant();

    @Override
    ESRandomAccessQuantizedByteVectorValues copy() throws IOException;
}
