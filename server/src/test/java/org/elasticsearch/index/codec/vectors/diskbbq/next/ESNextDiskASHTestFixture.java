/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.vectors.diskbbq.AshConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfFlushConfigSource;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Builds ASH formats and reaches their readers, for codec and search-layer tests.
 */
public final class ESNextDiskASHTestFixture {

    private ESNextDiskASHTestFixture() {}

    /**
     * The ASH format with the default projection, bits per dimension and clustering, and the given query bits.
     *
     * @param queryBitsPerDim bits per dimension of the quantized query, 0 for the float scoring path
     * @param sliceField the field to slice segments by, or {@code null} for an unsliced index
     */
    public static ESNextDiskASHVectorsFormat ashFormat(int queryBitsPerDim, @Nullable String sliceField) {
        IvfSegmentConfig.AshConfig ashConfig = IvfSegmentConfig.AshConfig.of(
            IvfSegmentConfig.AshConfig.DEFAULT_BITS_PER_DIM,
            queryBitsPerDim,
            IvfSegmentConfig.AshConfig.DEFAULT_PROJECTED_DIMS_FRACTION,
            IvfSegmentConfig.AshConfig.DEFAULT_MAX_PROJECTED_DIMS
        );
        return new ESNextDiskASHVectorsFormat(
            AshConfigResolver.fixed(ashConfig),
            ESNextDiskASHVectorsFormat.MIN_VECTORS_PER_CLUSTER,
            ESNextDiskASHVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
            DenseVectorFieldMapper.ElementType.FLOAT,
            false,
            null,
            1,
            null,
            -1,
            sliceField,
            IvfFlushConfigSource.empty(),
            IvfMergeConfigResolver.useCodecDefault(),
            false
        );
    }

    /** Returns the ASH reader of {@code field} in the segment of {@code leafReader}. */
    public static ESNextDiskASHVectorsReader ashReader(LeafReader leafReader, String field) {
        KnnVectorsReader vectorReader = ((CodecReader) leafReader).getVectorReader();
        if (vectorReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
            vectorReader = fieldsReader.getFieldReader(field);
        }
        assertThat(vectorReader, instanceOf(ESNextDiskASHVectorsReader.class));
        return (ESNextDiskASHVectorsReader) vectorReader;
    }
}
