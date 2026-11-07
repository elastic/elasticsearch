/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.xpack.ml.MachineLearningTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class OutlierDetectionAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "embedding";
    private static final int DIMS = 3;

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(MachineLearningTests.createTrialLicensedMachineLearning(Settings.EMPTY));
    }

    public void testKnownOutlierDetected() throws IOException {
        try (var directory = new ByteBuffersDirectory()) {
            try (var writer = new IndexWriter(directory, new IndexWriterConfig())) {
                addVector(writer, "normal1", new float[] { 0.1f, 0.1f, 0.1f });
                addVector(writer, "normal2", new float[] { 0.2f, 0.0f, 0.1f });
                addVector(writer, "normal3", new float[] { 0.0f, 0.2f, 0.1f });
                addVector(writer, "normal4", new float[] { 0.1f, 0.1f, 0.2f });
                addVector(writer, "normal5", new float[] { 0.15f, 0.15f, 0.15f });
                addVector(writer, "normal6", new float[] { 0.05f, 0.05f, 0.05f });
                addVector(writer, "normal7", new float[] { 0.12f, 0.08f, 0.11f });
                addVector(writer, "normal8", new float[] { 0.18f, 0.12f, 0.09f });
                addVector(writer, "outlier1", new float[] { 10.0f, 10.0f, 10.0f });
                writer.commit();
            }

            try (var reader = DirectoryReader.open(directory)) {
                var fieldType = new DenseVectorFieldMapper.DenseVectorFieldType(
                    FIELD_NAME,
                    IndexVersion.current(),
                    DenseVectorFieldMapper.ElementType.FLOAT,
                    DIMS,
                    true,
                    DenseVectorFieldMapper.VectorSimilarity.L2_NORM,
                    null,
                    Map.of(),
                    false
                );

                OutlierDetectionAggregationBuilder builder = new OutlierDetectionAggregationBuilder("test_outliers").setField(FIELD_NAME)
                    .setTopN(1)
                    .setNNeighbors(3)
                    .setSeed(42)
                    .setOverfetchFactor(3);

                InternalOutlierDetection result = searchAndReduce(reader, new AggTestConfig(builder, fieldType));

                assertNotNull(result);
                assertFalse("Should detect at least one outlier", result.getCandidates().isEmpty());

                OutlierCandidate topOutlier = result.getCandidates().get(0);
                assertEquals("outlier1", topOutlier.getDocId());
            }
        }
    }

    public void testSeedReproducibility() throws IOException {
        try (var directory = new ByteBuffersDirectory()) {
            try (var writer = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 20; i++) {
                    addVector(writer, "doc" + i, new float[] { (float) (Math.sin(i) * 0.5), (float) (Math.cos(i) * 0.5), 0.1f * i });
                }
                addVector(writer, "outlier", new float[] { 50.0f, 50.0f, 50.0f });
                writer.commit();
            }

            try (var reader = DirectoryReader.open(directory)) {
                var fieldType = new DenseVectorFieldMapper.DenseVectorFieldType(
                    FIELD_NAME,
                    IndexVersion.current(),
                    DenseVectorFieldMapper.ElementType.FLOAT,
                    DIMS,
                    true,
                    DenseVectorFieldMapper.VectorSimilarity.L2_NORM,
                    null,
                    Map.of(),
                    false
                );

                OutlierDetectionAggregationBuilder builder1 = new OutlierDetectionAggregationBuilder("test1").setField(FIELD_NAME)
                    .setTopN(3)
                    .setNNeighbors(3)
                    .setSeed(12345);

                OutlierDetectionAggregationBuilder builder2 = new OutlierDetectionAggregationBuilder("test2").setField(FIELD_NAME)
                    .setTopN(3)
                    .setNNeighbors(3)
                    .setSeed(12345);

                InternalOutlierDetection result1 = searchAndReduce(reader, new AggTestConfig(builder1, fieldType));
                InternalOutlierDetection result2 = searchAndReduce(reader, new AggTestConfig(builder2, fieldType));

                assertEquals(result1.getCandidates().size(), result2.getCandidates().size());
                for (int i = 0; i < result1.getCandidates().size(); i++) {
                    assertEquals(result1.getCandidates().get(i).getDocId(), result2.getCandidates().get(i).getDocId());
                    assertEquals(result1.getCandidates().get(i).getScore(), result2.getCandidates().get(i).getScore(), 0.001);
                }
            }
        }
    }

    public void testEmptyIndex() throws IOException {
        try (var directory = new ByteBuffersDirectory()) {
            try (var writer = new IndexWriter(directory, new IndexWriterConfig())) {
                writer.commit();
            }

            try (var reader = DirectoryReader.open(directory)) {
                var fieldType = new DenseVectorFieldMapper.DenseVectorFieldType(
                    FIELD_NAME,
                    IndexVersion.current(),
                    DenseVectorFieldMapper.ElementType.FLOAT,
                    DIMS,
                    true,
                    DenseVectorFieldMapper.VectorSimilarity.L2_NORM,
                    null,
                    Map.of(),
                    false
                );

                OutlierDetectionAggregationBuilder builder = new OutlierDetectionAggregationBuilder("test_empty").setField(FIELD_NAME)
                    .setTopN(5)
                    .setNNeighbors(3)
                    .setSeed(42);

                InternalOutlierDetection result = searchAndReduce(reader, new AggTestConfig(builder, fieldType));

                assertNotNull(result);
                assertTrue("Empty index should produce no outliers", result.getCandidates().isEmpty());
            }
        }
    }

    private static void addVector(IndexWriter writer, String id, float[] vector) throws IOException {
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Field.Store.YES));
        doc.add(new KnnFloatVectorField(FIELD_NAME, vector, VectorSimilarityFunction.EUCLIDEAN));
        writer.addDocument(doc);
    }
}
