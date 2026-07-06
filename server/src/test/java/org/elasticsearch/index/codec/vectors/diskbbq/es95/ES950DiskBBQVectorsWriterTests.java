/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.diskbbq.es95;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase.randomVector;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ES950DiskBBQVectorsWriterTests extends ESTestCase {

    static {
        LogConfigurator.configureESLogging();
    }

    public void testReadCentroidDataLoadsClusterSizesWithoutParents() throws Exception {
        try (IVFVectorsReader.CentroidData centroidData = readCentroidData(64, 16, 256)) {
            assertNotNull(centroidData);
            assertThat(centroidData.centroids().size(), lessThanOrEqualTo(16));
            assertClusterSizesAreLoaded(centroidData, 256);
        }
    }

    public void testReadCentroidDataLoadsClusterSizesWithParents() throws Exception {
        try (IVFVectorsReader.CentroidData centroidData = readCentroidData(64, 2, 512)) {
            assertNotNull(centroidData);
            assertThat(centroidData.centroids().size(), greaterThan(2));
            assertClusterSizesAreLoaded(centroidData, 512);
        }
    }

    private static void assertClusterSizesAreLoaded(IVFVectorsReader.CentroidData centroidData, int numDocs) {
        assertEquals(centroidData.centroids().size(), centroidData.clusterSizes().length);
        int totalAssignments = Arrays.stream(centroidData.clusterSizes()).sum();
        assertThat(totalAssignments, both(greaterThanOrEqualTo(numDocs)).and(lessThanOrEqualTo(numDocs * 2)));
        for (int clusterSize : centroidData.clusterSizes()) {
            assertThat(clusterSize, greaterThan(0));
        }
    }

    private IVFVectorsReader.CentroidData readCentroidData(int vectorsPerCluster, int centroidsPerParentCluster, int numDocs)
        throws Exception {
        try (
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(vectorsPerCluster, centroidsPerParentCluster, numDocs))
        ) {
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new KnnFloatVectorField("vector", randomVector(16), VectorSimilarityFunction.EUCLIDEAN));
                writer.addDocument(document);
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(writer)) {
                assertEquals("expected a single segment", 1, reader.leaves().size());
                LeafReader leafReader = reader.leaves().get(0).reader();
                KnnVectorsReader vectorReader = ((CodecReader) leafReader).getVectorReader();
                if (vectorReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                    vectorReader = fieldsReader.getFieldReader("vector");
                }
                assertThat(vectorReader, instanceOf(ES950DiskBBQVectorsReader.class));
                return ((ES950DiskBBQVectorsReader) vectorReader).readCentroidData("vector");
            }
        }
    }

    private IndexWriterConfig newIndexWriterConfig(int vectorsPerCluster, int centroidsPerParentCluster, int numDocs) {
        KnnVectorsFormat format = new ES950DiskBBQVectorsFormat(vectorsPerCluster, centroidsPerParentCluster);
        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        indexWriterConfig.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        // Ensure all documents are buffered into a single segment so the test can verify
        // the writer's cluster-size output deterministically. Without this, the randomized
        // IndexWriterConfig from ESTestCase can trigger intermediate flushes and produce
        // multiple segments, each containing only a subset of the documents.
        indexWriterConfig.setMaxBufferedDocs(numDocs + 1);
        indexWriterConfig.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        return indexWriterConfig;
    }
}
