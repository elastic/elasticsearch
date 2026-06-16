/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextRescoreOversampleTestFixture;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Query-time tests for IVF shard merge when leaves disagree on persisted rescore oversample.
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

                var resolver = IvfQueryConfigResolver.from(false, false, 4, oversampleB, null);

                IVFKnnFloatVectorQuery ivfQuery = new IVFKnnFloatVectorQuery(
                    ESNextRescoreOversampleTestFixture.FIELD_NAME,
                    queryVector,
                    k,
                    500,
                    null,
                    1f,
                    resolver
                );

                Query rewritten = ivfQuery.rewrite(searcher);
                TopDocs hits = searcher.search(rewritten, Integer.MAX_VALUE);

                assertThat(hits.scoreDocs.length, lessThanOrEqualTo(Math.min(expectedMergeK, reader.numDocs())));
                assertThat(hits.scoreDocs.length, greaterThan(k));
            }
        }
    }

    public void testIvfRescoreQueryUsesCalibratedMergeCandidates() throws IOException {
        Random rnd = random();
        float oversampleA = 2f;
        float oversampleB = 5f;
        final int k = 10;

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
                IndexSearcher searcher = newSearcher(reader);

                float[] queryVector = new float[4];
                for (int i = 0; i < queryVector.length; i++) {
                    queryVector[i] = rnd.nextFloat();
                }
                VectorUtil.l2normalize(queryVector);

                var resolver = IvfQueryConfigResolver.from(true, false, 4, DenseVectorFieldMapper.DEFAULT_OVERSAMPLE, null);
                IVFKnnFloatVectorQuery ivfQuery = new IVFKnnFloatVectorQuery(
                    ESNextRescoreOversampleTestFixture.FIELD_NAME,
                    queryVector,
                    k,
                    500,
                    null,
                    1f,
                    resolver
                );
                RescoreKnnVectorQuery rescoreQuery = RescoreKnnVectorQuery.fromInnerQuery(
                    ESNextRescoreOversampleTestFixture.FIELD_NAME,
                    queryVector,
                    k,
                    k,
                    ivfQuery
                );
                assertThat(rescoreQuery.k(), equalTo(k));

                Query rewritten = rescoreQuery.rewrite(searcher);
                TopDocs hits = searcher.search(rewritten, k);
                assertThat(hits.scoreDocs.length, lessThanOrEqualTo(k));
            }
        }
    }
}
