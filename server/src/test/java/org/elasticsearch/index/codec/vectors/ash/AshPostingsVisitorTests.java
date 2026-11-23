/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.search.vectors.CountingQueryTransformCache;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Sharing of the projected query between the visitors of segments with the same projection matrix, on the float and the
 * integer scoring paths. Segments are searched with the reader's default query bits, so this covers the float path that
 * searches through a segment do not reach.
 */
public class AshPostingsVisitorTests extends ESTestCase {

    private static final int ORIGINAL_DIM = 32;
    private static final int N_DIMS = 16;

    private static AshQueryProjection randomProjection() {
        return AshQueryProjection.quantize(
            UUID.randomUUID(),
            AshUtils.randomGaussians(random(), ORIGINAL_DIM * N_DIMS),
            ORIGINAL_DIM,
            N_DIMS
        );
    }

    private static AshPostingsVisitor<?> visitor(
        AshQueryProjection projection,
        float[] query,
        IndexInput input,
        int bitsPerDim,
        int queryBitsPerDim,
        CountingQueryTransformCache transforms
    ) throws IOException {
        float[] centroid = new float[ORIGINAL_DIM];
        return AshPostingsVisitor.create(
            projection,
            query,
            VectorSimilarityFunction.EUCLIDEAN,
            input,
            null,
            bitsPerDim,
            queryBitsPerDim,
            ord -> centroid,
            transforms
        );
    }

    public void testFloatPathSharesProjectionWhateverTheDocumentBits() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); IndexInput input = emptyInput(dir)) {
            AshQueryProjection projection = randomProjection();
            float[] query = VectorTestUtils.randomFloatVector(ORIGINAL_DIM);
            CountingQueryTransformCache transforms = new CountingQueryTransformCache();

            AshPostingsVisitor<?> first = visitor(projection, query, input, 1, 0, transforms);
            // segments with the same matrix but other document bits project the query the same way
            AshPostingsVisitor<?> second = visitor(projection, query, input, 2, 0, transforms);
            assertThat(transforms.computed(), equalTo(1));
            assertThat(second.scorerQuery, sameInstance(first.scorerQuery));

            AshPostingsVisitor<?> other = visitor(randomProjection(), query, input, 1, 0, transforms);
            assertThat(transforms.computed(), equalTo(2));
            assertThat(other.scorerQuery, not(sameInstance(first.scorerQuery)));
        }
    }

    public void testIntegerPathSharesProjectionPerDocumentBits() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); IndexInput input = emptyInput(dir)) {
            AshQueryProjection projection = randomProjection();
            float[] query = VectorTestUtils.randomFloatVector(ORIGINAL_DIM);
            CountingQueryTransformCache transforms = new CountingQueryTransformCache();
            int queryBits = 4;

            AshPostingsVisitor<?> first = visitor(projection, query, input, 1, queryBits, transforms);
            AshPostingsVisitor<?> again = visitor(projection, query, input, 1, queryBits, transforms);
            assertThat(transforms.computed(), equalTo(1));
            assertThat(again.scorerQuery, sameInstance(first.scorerQuery));

            // the quantized query depends on the document bits
            AshPostingsVisitor<?> otherBits = visitor(projection, query, input, 2, queryBits, transforms);
            assertThat(transforms.computed(), equalTo(2));
            assertThat(otherBits.scorerQuery, not(sameInstance(first.scorerQuery)));
        }
    }

    /**
     * A sliced query searches each named slice of a segment without per-slice centroids, such as a flushed one, with its own
     * sliced visitor: those visitors share the projected query.
     */
    public void testNamedSlicesOfASegmentShareProjection() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); IndexInput input = emptyInput(dir)) {
            AshQueryProjection projection = randomProjection();
            float[] query = VectorTestUtils.randomFloatVector(ORIGINAL_DIM);
            int bitsPerDim = randomIntBetween(1, 2);
            int queryBitsPerDim = randomFrom(0, 4);
            int docs = randomIntBetween(2, 20);
            List<float[]> vectors = new ArrayList<>();
            for (int i = 0; i < docs; i++) {
                vectors.add(VectorTestUtils.randomFloatVector(ORIGINAL_DIM));
            }
            FloatVectorValues values = FloatVectorValues.fromFloats(vectors, ORIGINAL_DIM);
            float[] centroid = new float[ORIGINAL_DIM];
            CountingQueryTransformCache transforms = new CountingQueryTransformCache();

            int sliceEnd = randomIntBetween(1, docs - 1);
            AshPostingsVisitor<?> first = slicedVisitor(
                projection,
                query,
                input,
                bitsPerDim,
                queryBitsPerDim,
                centroid,
                values,
                0,
                sliceEnd,
                transforms
            );
            AshPostingsVisitor<?> second = slicedVisitor(
                projection,
                query,
                input,
                bitsPerDim,
                queryBitsPerDim,
                centroid,
                values,
                sliceEnd,
                docs,
                transforms
            );
            assertThat(transforms.computed(), equalTo(1));
            assertThat(second.scorerQuery, sameInstance(first.scorerQuery));
        }
    }

    private static AshPostingsVisitor<?> slicedVisitor(
        AshQueryProjection projection,
        float[] query,
        IndexInput input,
        int bitsPerDim,
        int queryBitsPerDim,
        float[] centroid,
        FloatVectorValues values,
        int startDoc,
        int endDoc,
        CountingQueryTransformCache transforms
    ) throws IOException {
        return AshPostingsVisitor.createSliced(
            projection,
            query,
            VectorSimilarityFunction.EUCLIDEAN,
            input,
            null,
            bitsPerDim,
            queryBitsPerDim,
            ord -> centroid,
            values,
            startDoc,
            endDoc,
            transforms
        );
    }

    private static IndexInput emptyInput(Directory dir) throws IOException {
        try (IndexOutput out = dir.createOutput("postings", IOContext.DEFAULT)) {
            out.writeBytes(new byte[64], 64);
        }
        return dir.openInput("postings", IOContext.DEFAULT);
    }
}
