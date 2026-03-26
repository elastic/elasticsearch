/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.suggest.document.Completion104PostingsFormat;
import org.apache.lucene.search.suggest.document.SuggestField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.greaterThan;

public class IndexDiskUsageAnalyzerTests extends AbstractIndexDiskUsageAnalyzerTestCase {

    public void testStoredFields() throws Exception {
        try (Directory dir = createNewDirectory()) {
            final CodecMode codec = randomFrom(CodecMode.values());
            indexRandomly(dir, codec, between(100, 1000), doc -> {
                final double ratio = randomDouble();
                if (ratio <= 0.33) {
                    doc.add(new StoredField("sf1", randomAlphaOfLength(5)));
                }
                if (ratio <= 0.67) {
                    doc.add(new StoredField("sf2", randomAlphaOfLength(5)));
                }
                doc.add(new StoredField("sf3", randomAlphaOfLength(5)));
            });
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            final IndexDiskUsageStats perField = collectPerFieldStats(dir);
            assertFieldStats(
                "total",
                "stored field",
                stats.total().getStoredFieldBytes(),
                perField.total().getStoredFieldBytes(),
                0.01,
                1024
            );

            assertFieldStats(
                "sf1",
                "stored field",
                stats.getFields().get("sf1").getStoredFieldBytes(),
                stats.total().getStoredFieldBytes() / 6,
                0.01,
                512
            );

            assertFieldStats(
                "sf2",
                "stored field",
                stats.getFields().get("sf2").getStoredFieldBytes(),
                stats.total().getStoredFieldBytes() / 3,
                0.01,
                512
            );

            assertFieldStats(
                "sf3",
                "stored field",
                stats.getFields().get("sf3").getStoredFieldBytes(),
                stats.total().getStoredFieldBytes() / 2,
                0.01,
                512
            );
        }
    }

    public void testTermVectors() throws Exception {
        try (Directory dir = createNewDirectory()) {
            final CodecMode codec = randomFrom(CodecMode.values());
            indexRandomly(dir, codec, between(100, 1000), doc -> {
                final FieldType fieldType = randomTermVectorsFieldType();
                final double ratio = randomDouble();
                if (ratio <= 0.25) {
                    doc.add(new Field("v1", randomAlphaOfLength(5), fieldType));
                }
                if (ratio <= 0.50) {
                    doc.add(new Field("v2", randomAlphaOfLength(5), fieldType));
                }
                doc.add(new Field("v3", randomAlphaOfLength(5), fieldType));
            });
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            final IndexDiskUsageStats perField = collectPerFieldStats(dir);
            logger.info("--> stats {} per field {}", stats, perField);
            assertFieldStats(
                "total",
                "term vectors",
                stats.total().getTermVectorsBytes(),
                perField.total().getTermVectorsBytes(),
                0.01,
                1024
            );
            assertFieldStats(
                "v1",
                "term vectors",
                stats.getFields().get("v1").getTermVectorsBytes(),
                stats.total().getTermVectorsBytes() / 7,
                0.01,
                512
            );
            assertFieldStats(
                "v2",
                "term vectors",
                stats.getFields().get("v2").getTermVectorsBytes(),
                stats.total().getTermVectorsBytes() * 2 / 7,
                0.01,
                512
            );
            assertFieldStats(
                "v3",
                "term vectors",
                stats.getFields().get("v3").getTermVectorsBytes(),
                stats.total().getTermVectorsBytes() * 4 / 7,
                0.01,
                512
            );
        }
    }

    public void testBinaryPoints() throws Exception {
        try (Directory dir = createNewDirectory()) {
            final CodecMode codec = randomFrom(CodecMode.values());
            indexRandomly(dir, codec, between(100, 1000), doc -> {
                final double ratio = randomDouble();
                if (ratio <= 0.25) {
                    doc.add(new BinaryPoint("pt1", randomAlphaOfLength(5).getBytes(StandardCharsets.UTF_8)));
                }
                if (ratio <= 0.50) {
                    doc.add(new BinaryPoint("pt2", randomAlphaOfLength(5).getBytes(StandardCharsets.UTF_8)));
                }
                doc.add(new BinaryPoint("pt3", randomAlphaOfLength(5).getBytes(StandardCharsets.UTF_8)));
            });
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            final IndexDiskUsageStats perField = collectPerFieldStats(dir);
            logger.info("--> stats {} per field {}", stats, perField);
            assertFieldStats("total", "points", stats.total().getPointsBytes(), perField.total().getPointsBytes(), 0.01, 1024);
            assertFieldStats("pt1", "points", stats.getFields().get("pt1").getPointsBytes(), stats.total().getPointsBytes() / 7, 0.01, 512);
            assertFieldStats(
                "pt2",
                "points",
                stats.getFields().get("pt2").getPointsBytes(),
                stats.total().getPointsBytes() * 2 / 7,
                0.01,
                512
            );
            assertFieldStats(
                "pt3",
                "points",
                stats.getFields().get("pt3").getPointsBytes(),
                stats.total().getPointsBytes() * 4 / 7,
                0.01,
                512
            );
        }
    }

    public void testKnnVectors() throws Exception {
        try (Directory dir = createNewDirectory()) {
            final CodecMode codec = randomFrom(CodecMode.values());
            VectorSimilarityFunction similarity = randomFrom(VectorSimilarityFunction.values());
            int numDocs = between(1000, 5000);
            int dimension = between(10, 200);
            DenseVectorFieldMapper.ElementType elementType = randomFrom(DenseVectorFieldMapper.ElementType.values());

            if (elementType == DenseVectorFieldMapper.ElementType.FLOAT) {
                indexRandomly(dir, codec, numDocs, doc -> {
                    float[] vector = randomVector(dimension);
                    doc.add(new KnnFloatVectorField("vector", vector, similarity));
                });
            } else {
                indexRandomly(dir, codec, numDocs, doc -> {
                    byte[] vector = new byte[dimension];
                    random().nextBytes(vector);
                    doc.add(new KnnByteVectorField("vector", vector, similarity));
                });
            }
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            logger.info("--> stats {}", stats);

            // expected size of flat vector data
            long dataBytes = elementType == DenseVectorFieldMapper.ElementType.FLOAT
                ? ((long) numDocs * dimension * Float.BYTES)
                : ((long) numDocs * dimension);
            long indexBytesEstimate = (long) numDocs * (Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN / 4); // rough size of HNSW graph
            assertThat("numDocs=" + numDocs + ";dimension=" + dimension, stats.total().getKnnVectorsBytes(), greaterThan(dataBytes));
            long connectionOverhead = stats.total().getKnnVectorsBytes() - dataBytes;
            assertThat("numDocs=" + numDocs, connectionOverhead, greaterThan(indexBytesEstimate));
        }
    }

    public void testTriangle() throws Exception {
        try (Directory dir = createNewDirectory()) {
            final CodecMode codec = randomFrom(CodecMode.values());
            indexRandomly(dir, codec, between(100, 1000), doc -> {
                final double ratio = randomDouble();
                if (ratio <= 0.25) {
                    addFieldsToDoc(
                        doc,
                        LatLonShape.createIndexableFields("triangle_1", GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude())
                    );
                }
                if (ratio <= 0.50) {
                    addFieldsToDoc(
                        doc,
                        LatLonShape.createIndexableFields("triangle_2", GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude())
                    );
                }
                addFieldsToDoc(
                    doc,
                    LatLonShape.createIndexableFields("triangle_3", GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude())
                );
            });
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            final IndexDiskUsageStats perField = collectPerFieldStats(dir);
            logger.info("--> stats {} per field {}", stats, perField);
            assertFieldStats("total", "points", stats.total().getPointsBytes(), perField.total().getPointsBytes(), 0.01, 2048);
            assertFieldStats(
                "triangle_1",
                "points",
                stats.getFields().get("triangle_1").getPointsBytes(),
                stats.total().getPointsBytes() / 7,
                0.01,
                2048
            );
            assertFieldStats(
                "triangle_2",
                "triangle",
                stats.getFields().get("triangle_2").getPointsBytes(),
                stats.total().getPointsBytes() * 2 / 7,
                0.01,
                2048
            );
            assertFieldStats(
                "triangle_3",
                "triangle",
                stats.getFields().get("triangle_3").getPointsBytes(),
                stats.total().getPointsBytes() * 4 / 7,
                0.01,
                2048
            );
        }
    }

    public void testCompletionField() throws Exception {
        IndexWriterConfig config = new IndexWriterConfig().setCommitOnClose(true)
            .setUseCompoundFile(false)
            .setCodec(new Lucene104Codec(Lucene104Codec.Mode.BEST_SPEED) {
                @Override
                public PostingsFormat getPostingsFormatForField(String field) {
                    if (field.startsWith("suggest_")) {
                        return new Completion104PostingsFormat();
                    } else {
                        return super.postingsFormat();
                    }
                }
            });

        try (Directory dir = createNewDirectory()) {
            float docsWithSuggest1FieldRatio;
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                int numDocs = randomIntBetween(100, 1000);
                int numDocsWithSuggest1Field = 0;
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    if (randomDouble() < 0.5) {
                        numDocsWithSuggest1Field++;
                        doc.add(new SuggestField("suggest_1", randomAlphaOfLength(10), randomIntBetween(1, 20)));
                    }
                    doc.add(new SuggestField("suggest_2", randomAlphaOfLength(10), randomIntBetween(1, 20)));
                    writer.addDocument(doc);
                }
                docsWithSuggest1FieldRatio = (float) numDocsWithSuggest1Field / (numDocs + numDocsWithSuggest1Field);
            }
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            assertFieldStats(
                "suggest_1",
                "inverted_index",
                stats.getFields().get("suggest_1").getInvertedIndexBytes(),
                (long) (stats.total().totalBytes() * docsWithSuggest1FieldRatio),
                0.05,
                2048
            );

            assertFieldStats(
                "suggest_2",
                "inverted_index",
                stats.getFields().get("suggest_2").getInvertedIndexBytes(),
                (long) (stats.total().totalBytes() * (1 - docsWithSuggest1FieldRatio)),
                0.05,
                2048
            );

            final IndexDiskUsageStats perField = collectPerFieldStats(dir);
            assertFieldStats(
                "suggest_1",
                "inverted_index",
                stats.getFields().get("suggest_1").getInvertedIndexBytes(),
                perField.getFields().get("suggest_1").getInvertedIndexBytes(),
                0.05,
                2048
            );

            assertFieldStats(
                "suggest_2",
                "inverted_index",
                stats.getFields().get("suggest_2").getInvertedIndexBytes(),
                perField.getFields().get("suggest_2").getInvertedIndexBytes(),
                0.05,
                2048
            );
        }
    }

    public void testMixedFields() throws Exception {
        try (Directory dir = createNewDirectory()) {
            CodecMode codecMode = randomFrom(CodecMode.values());
            final CodecMode codec = randomFrom(CodecMode.values());
            indexRandomly(dir, codec, between(100, 1000), IndexDiskUsageAnalyzerTests::addRandomFields);
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            logger.info("--> stats {}", stats);
            try (Directory perFieldDir = createNewDirectory()) {
                rewriteIndexWithPerFieldCodec(dir, codecMode, perFieldDir, field -> new Lucene90DocValuesFormat());
                final IndexDiskUsageStats perFieldStats = collectPerFieldStats(perFieldDir);
                assertStats(stats, perFieldStats);
                assertStats(IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(perFieldDir), () -> {}), perFieldStats);
            }
        }
    }

    public void testDocValuesFieldWithDocValueSkippers() throws Exception {
        try (Directory dir = createNewDirectory()) {
            var codecMode = randomFrom(CodecMode.values());
            indexRandomly(dir, codecMode, between(100, 1000), doc -> addRandomDocValuesField(doc, true));
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            logger.info("--> stats {}", stats);
            try (Directory perFieldDir = createNewDirectory()) {
                rewriteIndexWithPerFieldCodec(dir, codecMode, perFieldDir, field -> new Lucene90DocValuesFormat());
                final IndexDiskUsageStats perFieldStats = collectPerFieldStats(perFieldDir);
                assertStats(stats, perFieldStats);
                assertStats(IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(perFieldDir), () -> {}), perFieldStats);
            }
        }
    }

}
