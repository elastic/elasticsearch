/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
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

public class ESNextDiskBBQVectorsWriterTests extends ESTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
    }

    public void testReadCentroidDataLoadsClusterSizesWithoutParents() throws Exception {
        IVFVectorsReader.CentroidData centroidData = readCentroidData(64, 16, 256);

        assertNotNull(centroidData);
        assertTrue(centroidData.centroids().length <= 16);
        assertClusterSizesAreLoaded(centroidData, 256);
    }

    public void testReadCentroidDataLoadsClusterSizesWithParents() throws Exception {
        IVFVectorsReader.CentroidData centroidData = readCentroidData(64, 2, 512);

        assertNotNull(centroidData);
        assertTrue(centroidData.centroids().length > 2);
        assertClusterSizesAreLoaded(centroidData, 512);
    }

    private static void assertClusterSizesAreLoaded(IVFVectorsReader.CentroidData centroidData, int numDocs) {
        assertEquals(centroidData.centroids().length, centroidData.clusterSizes().length);
        int totalAssignments = Arrays.stream(centroidData.clusterSizes()).sum();
        assertTrue(totalAssignments >= numDocs);
        assertTrue(totalAssignments <= numDocs * 2);
        for (int clusterSize : centroidData.clusterSizes()) {
            assertTrue(clusterSize > 0);
        }
    }

    private IVFVectorsReader.CentroidData readCentroidData(int vectorsPerCluster, int centroidsPerParentCluster, int numDocs)
        throws Exception {
        try (
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(vectorsPerCluster, centroidsPerParentCluster))
        ) {
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new KnnFloatVectorField("vector", randomVector(16), VectorSimilarityFunction.EUCLIDEAN));
                writer.addDocument(document);
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(writer)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo("vector");
                KnnVectorsReader vectorReader = ((CodecReader) leafReader).getVectorReader();
                if (vectorReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                    vectorReader = fieldsReader.getFieldReader("vector");
                }
                assertTrue(vectorReader instanceof ESNextDiskBBQVectorsReader);
                return ((ESNextDiskBBQVectorsReader) vectorReader).readCentroidData(fieldInfo);
            }
        }
    }

    private IndexWriterConfig newIndexWriterConfig(int vectorsPerCluster, int centroidsPerParentCluster) {
        KnnVectorsFormat format = new ESNextDiskBBQVectorsFormat(vectorsPerCluster, centroidsPerParentCluster, null);
        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        indexWriterConfig.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        return indexWriterConfig;
    }

    private float[] randomVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }
}
