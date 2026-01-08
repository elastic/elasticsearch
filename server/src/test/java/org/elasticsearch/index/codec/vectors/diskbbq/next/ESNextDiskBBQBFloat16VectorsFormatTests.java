/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.BaseBFloat16KnnVectorsFormatTestCase;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.junit.AssumptionViolatedException;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat.MIN_CENTROIDS_PER_PARENT_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat.MIN_VECTORS_PER_CLUSTER;
import static org.hamcrest.Matchers.equalTo;

public class ESNextDiskBBQBFloat16VectorsFormatTests extends BaseBFloat16KnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    private KnnVectorsFormat format;

    @Before
    @Override
    public void setUp() throws Exception {
        ESNextDiskBBQVectorsFormat.QuantEncoding encoding = ESNextDiskBBQVectorsFormat.QuantEncoding.values()[random().nextInt(
            ESNextDiskBBQVectorsFormat.QuantEncoding.values().length
        )];
        if (rarely()) {
            format = new ESNextDiskBBQVectorsFormat(
                encoding,
                random().nextInt(2 * MIN_VECTORS_PER_CLUSTER, ES920DiskBBQVectorsFormat.MAX_VECTORS_PER_CLUSTER),
                random().nextInt(8, ES920DiskBBQVectorsFormat.MAX_CENTROIDS_PER_PARENT_CLUSTER),
                DenseVectorFieldMapper.ElementType.BFLOAT16,
                random().nextBoolean()
            );
        } else {
            // run with low numbers to force many clusters with parents
            format = new ESNextDiskBBQVectorsFormat(
                encoding,
                random().nextInt(MIN_VECTORS_PER_CLUSTER, 2 * MIN_VECTORS_PER_CLUSTER),
                random().nextInt(MIN_CENTROIDS_PER_PARENT_CLUSTER, 8),
                DenseVectorFieldMapper.ElementType.BFLOAT16,
                random().nextBoolean()
            );
        }
        super.setUp();
    }

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysKnnVectorsFormat(format);
    }

    @Override
    protected VectorSimilarityFunction randomSimilarity() {
        return RandomPicks.randomFrom(
            random(),
            List.of(
                VectorSimilarityFunction.DOT_PRODUCT,
                VectorSimilarityFunction.EUCLIDEAN,
                VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT
            )
        );
    }

    @Override
    public void testSearchWithVisitedLimit() {
        throw new AssumptionViolatedException("ivf doesn't enforce visitation limit");
    }

    @Override
    public void testAdvance() throws Exception {
        // TODO re-enable with hierarchical IVF, clustering as it is is flaky
    }

    @Override
    protected void assertOffHeapByteSize(LeafReader r, String fieldName) throws IOException {
        var fieldInfo = r.getFieldInfos().fieldInfo(fieldName);

        if (r instanceof CodecReader codecReader) {
            KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
            if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                knnVectorsReader = fieldsReader.getFieldReader(fieldName);
            }
            var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
            long totalByteSize = offHeap.values().stream().mapToLong(Long::longValue).sum();
            assertThat(offHeap.size(), equalTo(3));
            assertThat(totalByteSize, equalTo(offHeap.values().stream().mapToLong(Long::longValue).sum()));
        } else {
            throw new AssertionError("unexpected:" + r.getClass());
        }
    }
}
