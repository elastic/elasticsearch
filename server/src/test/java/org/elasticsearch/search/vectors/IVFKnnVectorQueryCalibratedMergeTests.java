/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.vectors.diskbbq.next.CalibrationAwareReader;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextRescoreOversampleTestFixture;
import org.elasticsearch.index.codec.vectors.diskbbq.next.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.next.IvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.next.IvfSegmentConfig;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Random;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Ensures IVF shard merge retains {@code ceil(k * max(segment rescore oversample))} approximate neighbors
 * when segments disagree on calibrated oversampling (Disk BBQ).
 */
public class IVFKnnVectorQueryCalibratedMergeTests extends ESTestCase {

    public void testShardMergeKeepsCandidatesPerMaxPersistedRescoreOversample() throws IOException {
        Random rnd = random();
        float oversampleA = 2f;
        float oversampleB = 5f;
        final int k = 10;
        final int expectedMergeK = (int) Math.ceil(k * oversampleB);

        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    rnd,
                    4,
                    64,
                    oversampleA,
                    oversampleB,
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                ESNextRescoreOversampleTestFixture.assertLeafOversamples(reader, oversampleA, oversampleB);

                IndexSearcher searcher = newSearcher(reader);

                float[] queryVector = new float[4];
                for (int i = 0; i < queryVector.length; i++) {
                    queryVector[i] = rnd.nextFloat();
                }
                VectorUtil.l2normalize(queryVector);

                IvfQueryConfigResolver resolver = (fieldInfo, leafReader) -> {
                    SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(leafReader);
                    assertNotNull(segmentReader);
                    KnnVectorsReader vectorsReader = segmentReader.getVectorReader();
                    if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perField) {
                        vectorsReader = perField.getFieldReader(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                    }
                    if (vectorsReader instanceof CalibrationAwareReader calibrationAwareReader) {
                        float ov = calibrationAwareReader.getOversampleFactor(fieldInfo);
                        ESNextDiskBBQVectorsFormat.QuantEncoding encoding = calibrationAwareReader.getQuantEncoding(fieldInfo);
                        boolean precondition = calibrationAwareReader.shouldPrecondition(fieldInfo);
                        return new IvfSegmentConfig(encoding, precondition, ov);
                    }
                    throw new AssertionError("expected CalibrationAwareReader on vectors reader");
                };

                IVFKnnFloatVectorQuery ivfQuery = new IVFKnnFloatVectorQuery(
                    ESNextRescoreOversampleTestFixture.FIELD_NAME,
                    queryVector,
                    k,
                    500,
                    null,
                    1f,
                    false,
                    resolver
                );

                Query rewritten = ivfQuery.rewrite(searcher);
                TopDocs hits = searcher.search(rewritten, Integer.MAX_VALUE);

                assertThat(hits.scoreDocs.length, lessThanOrEqualTo(Math.min(expectedMergeK, reader.numDocs())));
                assertThat(hits.scoreDocs.length, greaterThan(k));
            }
        }
    }
}
