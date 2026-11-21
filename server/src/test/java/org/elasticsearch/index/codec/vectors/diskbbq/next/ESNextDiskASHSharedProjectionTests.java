/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.search.vectors.CountingQueryTransformCache;
import org.elasticsearch.search.vectors.IVFKnnSearchStrategy;
import org.elasticsearch.search.vectors.QueryTransformCache;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Reuse of a query's projection across the segments of a query that share the ASH projection matrix W, through
 * {@link QueryTransformCache}.
 */
public class ESNextDiskASHSharedProjectionTests extends ESTestCase {

    private static final String FIELD = "f";
    private static final int DIMS = 128;

    private static IndexWriterConfig writerConfig() {
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setCodec(
            TestUtil.alwaysKnnVectorsFormat(
                new ESNextDiskASHVectorsFormat(
                    ESNextDiskASHVectorsFormat.MIN_VECTORS_PER_CLUSTER,
                    ESNextDiskASHVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
                    null
                )
            )
        );
        // keep each flush its own segment
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        iwc.setRAMBufferSizeMB(1024);
        return iwc;
    }

    private static void flush(IndexWriter w) throws IOException {
        int docs = randomIntBetween(500, 1000);
        for (int i = 0; i < docs; i++) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField(FIELD, VectorTestUtils.randomFloatVector(DIMS), VectorSimilarityFunction.EUCLIDEAN));
            w.addDocument(doc);
        }
        w.commit();
    }

    /** The distinct projection matrix ids of the segments. */
    private static Set<UUID> distinctMatrixIds(DirectoryReader reader) {
        Set<UUID> ids = new HashSet<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            FieldInfo fieldInfo = ctx.reader().getFieldInfos().fieldInfo(FIELD);
            ids.add(ESNextDiskASHVectorsFormatTests.ashReader(ctx.reader(), FIELD).getProjectionMatrix(fieldInfo).id());
        }
        return ids;
    }

    public void testQueryIsProjectedOncePerSharedProjection() throws IOException {
        try (Directory source = newDirectory(); Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(source, writerConfig())) {
                flush(w);
            }
            int copies = randomIntBetween(2, 4);
            try (IndexWriter w = new IndexWriter(dir, writerConfig())) {
                // copies of a segment keep its projection matrix, and so share it
                for (int c = 0; c < copies; c++) {
                    w.addIndexes(source);
                }
                try (DirectoryReader reader = DirectoryReader.open(w)) {
                    assertThat(reader.leaves(), hasSize(copies));
                    assertThat(distinctMatrixIds(reader), hasSize(1));
                    assertProjectedOncePerDistinctMatrix(reader, 1);
                }
            }
        }
    }

    public void testQueryIsProjectedPerDistinctProjection() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, writerConfig())) {
            int segments = randomIntBetween(2, 4);
            for (int s = 0; s < segments; s++) {
                flush(w);
            }
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                assertThat(reader.leaves(), hasSize(segments));
                assertThat("each flush learns its own projection", distinctMatrixIds(reader), hasSize(segments));
                assertProjectedOncePerDistinctMatrix(reader, segments);
            }
        }
    }

    public void testSegmentSearchedRepeatedlyProjectsOnce() throws IOException {
        // a sliced query searches a segment once for each named slice it requests, with the same query
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, writerConfig())) {
            flush(w);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                float[] query = VectorTestUtils.randomFloatVector(DIMS);
                TopDocs expected = search(leafReader, query, QueryTransformCache.NONE);
                CountingQueryTransformCache transforms = new CountingQueryTransformCache();
                int searches = randomIntBetween(2, 5);
                for (int i = 0; i < searches; i++) {
                    assertSameResults(expected, search(leafReader, query, transforms));
                }
                assertThat(transforms.computed(), equalTo(1));
            }
        }
    }

    /** Searches every segment with and without shared transforms, and checks results and the number of projections. */
    private static void assertProjectedOncePerDistinctMatrix(DirectoryReader reader, int distinctMatrices) throws IOException {
        for (int q = 0; q < 5; q++) {
            float[] query = VectorTestUtils.randomFloatVector(DIMS);
            CountingQueryTransformCache transforms = new CountingQueryTransformCache();
            for (LeafReaderContext ctx : reader.leaves()) {
                assertSameResults(search(ctx.reader(), query, QueryTransformCache.NONE), search(ctx.reader(), query, transforms));
            }
            assertThat(transforms.computed(), equalTo(distinctMatrices));
        }
    }

    private static TopDocs search(LeafReader leafReader, float[] query, QueryTransformCache transforms) throws IOException {
        int k = 10;
        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(1.0f, k, k, null, transforms);
        TopKnnCollector collector = new TopKnnCollector(k, Integer.MAX_VALUE, strategy);
        leafReader.searchNearestVectors(FIELD, query, collector, AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()));
        return collector.topDocs();
    }

    private static void assertSameResults(TopDocs expected, TopDocs actual) {
        assertThat(expected.scoreDocs.length, equalTo(10));
        assertThat(actual.scoreDocs.length, equalTo(expected.scoreDocs.length));
        for (int i = 0; i < expected.scoreDocs.length; i++) {
            ScoreDoc e = expected.scoreDocs[i];
            ScoreDoc a = actual.scoreDocs[i];
            assertThat(a.doc, equalTo(e.doc));
            assertThat(a.score, equalTo(e.score));
        }
    }
}
