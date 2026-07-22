/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.vectors.BaseByteKnnVectorsFormatTestCase;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.junit.Before;

import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.DEFAULT_PRECONDITIONING_BLOCK_DIMENSION;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MAX_CENTROIDS_PER_PARENT_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MAX_PRECONDITIONING_BLOCK_DIMS;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MAX_VECTORS_PER_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MIN_CENTROIDS_PER_PARENT_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MIN_PRECONDITIONING_BLOCK_DIMS;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MIN_VECTORS_PER_CLUSTER;

/**
 * Tests for byte vector support with the ESNext IVF disk BBQ vectors format.
 */
public class ESNextDiskBBQByteVectorsFormatTests extends BaseByteKnnVectorsFormatTestCase {

    private KnnVectorsFormat format;

    @Before
    @Override
    public void setUp() throws Exception {
        QuantEncoding encoding = QuantEncoding.values()[random().nextInt(QuantEncoding.values().length)];
        if (rarely()) {
            format = new ESNextDiskBBQVectorsFormat(
                encoding,
                random().nextInt(2 * MIN_VECTORS_PER_CLUSTER, MAX_VECTORS_PER_CLUSTER),
                random().nextInt(8, MAX_CENTROIDS_PER_PARENT_CLUSTER),
                DenseVectorFieldMapper.ElementType.BYTE,
                false,
                null,
                1,
                false,
                DEFAULT_PRECONDITIONING_BLOCK_DIMENSION,
                null
            );
        } else if (rarely()) {
            format = new ESNextDiskBBQVectorsFormat(
                encoding,
                random().nextInt(MIN_VECTORS_PER_CLUSTER, MAX_VECTORS_PER_CLUSTER),
                random().nextInt(MIN_CENTROIDS_PER_PARENT_CLUSTER, MAX_CENTROIDS_PER_PARENT_CLUSTER),
                DenseVectorFieldMapper.ElementType.BYTE,
                false,
                null,
                1,
                true,
                random().nextInt(MIN_PRECONDITIONING_BLOCK_DIMS, MAX_PRECONDITIONING_BLOCK_DIMS),
                null
            );
        } else {
            // run with low numbers to force many clusters with parents
            format = new ESNextDiskBBQVectorsFormat(
                encoding,
                random().nextInt(MIN_VECTORS_PER_CLUSTER, 2 * MIN_VECTORS_PER_CLUSTER),
                random().nextInt(MIN_CENTROIDS_PER_PARENT_CLUSTER, 8),
                DenseVectorFieldMapper.ElementType.BYTE,
                false,
                null,
                1,
                false,
                DEFAULT_PRECONDITIONING_BLOCK_DIMENSION,
                null
            );
        }
        super.setUp();
    }

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysKnnVectorsFormat(format);
    }
}
