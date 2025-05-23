/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.suggest.document.Completion101PostingsFormat;
import org.apache.lucene.search.suggest.document.SuggestField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.postings.ES812PostingsFormat;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class IndexDiskUsageAnalyzerTests extends ESTestCase {
    private static final int DEFAULT_VECTOR_DIMENSION = 128;

    protected static Directory createNewDirectory() {
        final Directory dir = LuceneTestCase.newDirectory();
        if (randomBoolean()) {
            return new FilterDirectory(dir) {
                @Override
                public void close() throws IOException {
                    try {
                        analyzeDiskUsageAfterDeleteRandomDocuments(dir);
                    } finally {
                        super.close();
                    }
                }
            };
        } else {
            return dir;
        }
    }

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

            indexRandomly(dir, codec, numDocs, doc -> {
                float[] vector = randomVector(dimension);
                doc.add(new KnnFloatVectorField("vector", vector, similarity));
            });
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            logger.info("--> stats {}", stats);

            long dataBytes = (long) numDocs * dimension * Float.BYTES; // size of flat vector data
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
            .setCodec(new Lucene103Codec(Lucene103Codec.Mode.BEST_SPEED) {
                @Override
                public PostingsFormat getPostingsFormatForField(String field) {
                    if (field.startsWith("suggest_")) {
                        return new Completion101PostingsFormat();
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
            final CodecMode codec = randomFrom(CodecMode.values());
            indexRandomly(dir, codec, between(100, 1000), IndexDiskUsageAnalyzerTests::addRandomFields);
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            logger.info("--> stats {}", stats);
            try (Directory perFieldDir = createNewDirectory()) {
                rewriteIndexWithPerFieldCodec(dir, codec, perFieldDir);
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
                rewriteIndexWithPerFieldCodec(dir, codecMode, perFieldDir);
                final IndexDiskUsageStats perFieldStats = collectPerFieldStats(perFieldDir);
                assertStats(stats, perFieldStats);
                assertStats(IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(perFieldDir), () -> {}), perFieldStats);
            }
        }
    }

    private static void addFieldsToDoc(Document doc, IndexableField[] fields) {
        for (IndexableField field : fields) {
            doc.add(field);
        }
    }

    enum CodecMode {
        BEST_SPEED {
            @Override
            Lucene103Codec.Mode mode() {
                return Lucene103Codec.Mode.BEST_SPEED;
            }
        },

        BEST_COMPRESSION {
            @Override
            Lucene103Codec.Mode mode() {
                return Lucene103Codec.Mode.BEST_COMPRESSION;
            }
        };

        abstract Lucene103Codec.Mode mode();
    }

    static void indexRandomly(Directory directory, CodecMode codecMode, int numDocs, Consumer<Document> addFields) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig().setCommitOnClose(true)
            .setUseCompoundFile(randomBoolean())
            .setCodec(new Lucene103Codec(codecMode.mode()));
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < numDocs; i++) {
                final Document doc = new Document();
                addFields.accept(doc);
                writer.addDocument(doc);
            }
        }
    }

    static void addRandomDocValuesField(Document doc, boolean indexed) {
        if (randomBoolean()) {
            int val = random().nextInt(1024);
            doc.add(indexed ? NumericDocValuesField.indexedField("ndv", val) : new NumericDocValuesField("ndv", val));
        }
        if (randomBoolean() && indexed == false) {
            doc.add(new BinaryDocValuesField("bdv", new BytesRef(randomAlphaOfLength(3))));
        }
        if (randomBoolean()) {
            var value = new BytesRef(randomAlphaOfLength(3));
            doc.add(indexed ? SortedDocValuesField.indexedField("sdv", value) : new SortedDocValuesField("sdv", value));
        }
        int numValues = random().nextInt(5);
        for (int i = 0; i < numValues; ++i) {
            var value = new BytesRef(randomAlphaOfLength(3));
            doc.add(indexed ? SortedSetDocValuesField.indexedField("ssdv", value) : new SortedSetDocValuesField("ssdv", value));
        }
        numValues = random().nextInt(5);
        for (int i = 0; i < numValues; ++i) {
            int value = random().nextInt(1024);
            doc.add(indexed ? SortedNumericDocValuesField.indexedField("sndv", value) : new SortedNumericDocValuesField("sndv", value));
        }
    }

    static void addRandomPostings(Document doc) {
        for (IndexOptions opts : IndexOptions.values()) {
            if (opts == IndexOptions.NONE) {
                continue;
            }
            FieldType ft = new FieldType();
            ft.setIndexOptions(opts);
            ft.freeze();
            final int numFields = random().nextInt(5);
            for (int j = 0; j < numFields; ++j) {
                doc.add(new Field("f_" + opts, randomAlphaOfLength(5), ft));
            }
        }
    }

    static void addRandomStoredFields(Document doc, int numFields) {
        final int numValues = random().nextInt(3);
        for (int i = 0; i < numValues; ++i) {
            final String field = "sf-" + between(1, numFields);
            if (randomBoolean()) {
                doc.add(new StoredField(field, randomAlphaOfLength(5)));
            } else {
                doc.add(new StoredField(field, randomLong()));
            }
        }
    }

    static void addRandomIntLongPoints(Document doc) {
        final int numValues = random().nextInt(5);
        for (int i = 0; i < numValues; i++) {
            if (randomBoolean()) {
                doc.add(new IntPoint("int_point_" + randomIntBetween(1, 2), random().nextInt()));
            }
            if (randomBoolean()) {
                doc.add(new LongPoint("long_point_" + randomIntBetween(1, 2), random().nextLong()));
            }
        }
    }

    static FieldType randomTermVectorsFieldType() {
        FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
        fieldType.setStoreTermVectors(true);
        fieldType.setStoreTermVectorPositions(randomBoolean());
        fieldType.setStoreTermVectorOffsets(randomBoolean());
        // TODO: add payloads
        fieldType.setStoreTermVectorPayloads(false);
        return fieldType;
    }

    static void addRandomTermVectors(Document doc) {
        int numFields = randomFrom(1, 3);
        for (int f = 0; f < numFields; f++) {
            doc.add(new Field("vector-" + f, randomAlphaOfLength(5), randomTermVectorsFieldType()));
        }
    }

    static void addRandomKnnVectors(Document doc) {
        int numFields = randomFrom(1, 3);
        for (int f = 0; f < numFields; f++) {
            doc.add(new KnnFloatVectorField("knnvector-" + f, randomVector(DEFAULT_VECTOR_DIMENSION)));
        }
    }

    private static float[] randomVector(int dimension) {
        float[] vec = new float[dimension];
        for (int i = 0; i < vec.length; i++) {
            vec[i] = randomFloat();
        }
        return vec;
    }

    static void addRandomFields(Document doc) {
        if (randomBoolean()) {
            addRandomDocValuesField(doc, false);
        }
        if (randomBoolean()) {
            addRandomPostings(doc);
        }
        if (randomBoolean()) {
            addRandomIntLongPoints(doc);
        }
        if (randomBoolean()) {
            final int numValues = random().nextInt(5);
            for (int i = 0; i < numValues; i++) {
                addFieldsToDoc(
                    doc,
                    LatLonShape.createIndexableFields(
                        "triangle_" + randomIntBetween(1, 2),
                        GeoTestUtil.nextLatitude(),
                        GeoTestUtil.nextLongitude()
                    )
                );
            }
        }
        if (randomBoolean()) {
            addRandomStoredFields(doc, between(1, 3));
        }
        if (randomBoolean()) {
            addRandomTermVectors(doc);
        }
        if (randomBoolean()) {
            addRandomKnnVectors(doc);
        }
    }

    static class FieldLookup {
        private final Map<String, FieldInfo> dvSuffixes = new HashMap<>();
        private final Map<String, FieldInfo> postingsSuffixes = new HashMap<>();
        private final Map<String, FieldInfo> vectorSuffixes = new HashMap<>();

        FieldLookup(FieldInfos fieldInfos) {
            for (FieldInfo field : fieldInfos) {
                Map<String, String> attributes = field.attributes();
                if (attributes != null) {
                    String postingsSuffix = attributes.get(PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY);
                    if (postingsSuffix != null) {
                        postingsSuffixes.put(postingsSuffix, field);
                    }
                    String dvSuffix = attributes.get(PerFieldDocValuesFormat.PER_FIELD_SUFFIX_KEY);
                    if (dvSuffix != null) {
                        dvSuffixes.put(dvSuffix, field);
                    }
                    String vectorSuffix = attributes.get(PerFieldKnnVectorsFormat.PER_FIELD_SUFFIX_KEY);
                    if (vectorSuffix != null) {
                        vectorSuffixes.put(vectorSuffix, field);
                    }
                }
            }
        }

        /**
         * Returns the codec suffix from this file name, or null if there is no suffix.
         */
        private static String parseSuffix(String filename) {
            if (filename.startsWith("_") == false) {
                return null;
            }
            String[] parts = IndexFileNames.stripExtension(filename).substring(1).split("_");
            // 4 cases:
            // segment.ext
            // segment_gen.ext
            // segment_codec_suffix.ext
            // segment_gen_codec_suffix.ext
            if (parts.length == 3) {
                return parts[2];
            } else if (parts.length == 4) {
                return parts[3];
            } else {
                return null;
            }
        }

        String getDocValuesField(String fileName) {
            final String suffix = parseSuffix(fileName);
            final FieldInfo field = dvSuffixes.get(suffix);
            assertThat("dvSuffix[" + dvSuffixes + "] fileName[" + fileName + "]", field, notNullValue());
            return field.name;
        }

        String getPostingsField(String fileName) {
            final String suffix = parseSuffix(fileName);
            final FieldInfo field = postingsSuffixes.get(suffix);
            assertThat("postingsSuffixes[" + postingsSuffixes + "] fileName[" + fileName + "]", field, notNullValue());
            return field.name;
        }

        String getVectorsField(String fileName) {
            final String suffix = parseSuffix(fileName);
            final FieldInfo field = vectorSuffixes.get(suffix);
            assertThat("vectorSuffixes[" + vectorSuffixes + "] fileName[" + fileName + "]", field, notNullValue());
            return field.name;
        }
    }

    static void rewriteIndexWithPerFieldCodec(Directory source, CodecMode mode, Directory dst) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(source)) {
            IndexWriterConfig config = new IndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
                .setUseCompoundFile(randomBoolean())
                .setCodec(new Lucene103Codec(mode.mode()) {
                    @Override
                    public PostingsFormat getPostingsFormatForField(String field) {
                        return new ES812PostingsFormat();
                    }

                    @Override
                    public DocValuesFormat getDocValuesFormatForField(String field) {
                        return new Lucene90DocValuesFormat();
                    }

                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return new Lucene99HnswVectorsFormat();
                    }

                    @Override
                    public String toString() {
                        return super.toString();
                    }
                })
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            try (IndexWriter writer = new IndexWriter(dst, config)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    final SegmentReader segmentReader = Lucene.segmentReader(leaf.reader());
                    writer.addIndexes(segmentReader);
                }
                writer.commit();
            }
        }
    }

    static IndexDiskUsageStats collectPerFieldStats(Directory directory) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            final IndexDiskUsageStats stats = new IndexDiskUsageStats(IndexDiskUsageAnalyzer.getIndexSize(lastCommit(directory)));
            for (LeafReaderContext leaf : reader.leaves()) {
                collectPerFieldStats(Lucene.segmentReader(leaf.reader()), stats);
            }
            return stats;
        }
    }

    static void collectPerFieldStats(SegmentReader reader, IndexDiskUsageStats stats) throws IOException {
        final SegmentInfo sis = reader.getSegmentInfo().info;
        final String[] files;
        final Directory directory;
        if (sis.getUseCompoundFile()) {
            directory = sis.getCodec().compoundFormat().getCompoundReader(reader.directory(), sis);
            files = directory.listAll();
        } else {
            directory = reader.directory();
            files = sis.files().toArray(new String[0]);
        }
        final FieldLookup fieldLookup = new FieldLookup(reader.getFieldInfos());
        try {
            for (String file : files) {
                final LuceneFilesExtensions ext = LuceneFilesExtensions.fromFile(file);
                if (ext == null) {
                    continue;
                }
                final long bytes = directory.fileLength(file);
                switch (ext) {
                    case DVD, DVM -> stats.addDocValues(fieldLookup.getDocValuesField(file), bytes);
                    case TIM, TIP, TMD, DOC, POS, PAY -> stats.addInvertedIndex(fieldLookup.getPostingsField(file), bytes);
                    case KDI, KDD, KDM, DIM -> stats.addPoints("_all_points_fields", bytes);
                    case FDT, FDX, FDM ->
                        // We don't have per field Codec for stored, vector, and norms field
                        stats.addStoredField("_all_stored_fields", bytes);
                    case TVX, TVD -> stats.addTermVectors("_all_vectors_fields", bytes);
                    case NVD, NVM -> stats.addNorms("_all_norms_fields", bytes);
                    case VEM, VEMF, VEC, VEX, VEQ, VEMQ -> stats.addKnnVectors(fieldLookup.getVectorsField(file), bytes);
                }
            }
        } finally {
            if (directory != reader.directory()) {
                IOUtils.close(directory);
            }
        }
    }

    private static void assertStats(IndexDiskUsageStats actualStats, IndexDiskUsageStats perFieldStats) {
        final List<String> fields = actualStats.getFields().keySet().stream().sorted().toList();
        for (String field : fields) {
            IndexDiskUsageStats.PerFieldDiskUsage actualField = actualStats.getFields().get(field);
            IndexDiskUsageStats.PerFieldDiskUsage expectedField = perFieldStats.getFields().get(field);
            if (expectedField == null) {
                assertThat(actualField.getDocValuesBytes(), equalTo(0L));
                assertThat(actualField.getInvertedIndexBytes(), equalTo(0L));
                continue;
            }
            // Allow difference up to 2.5KB as we can load up to 256 long values in the table for numeric docValues
            assertFieldStats(field, "doc values", actualField.getDocValuesBytes(), expectedField.getDocValuesBytes(), 0.01, 2560);
            assertFieldStats(
                field,
                "inverted index",
                actualField.getInvertedIndexBytes(),
                expectedField.getInvertedIndexBytes(),
                0.01,
                2048
            );

            assertFieldStats(field, "knn vectors", actualField.getKnnVectorsBytes(), expectedField.getKnnVectorsBytes(), 0.01, 1024);
        }
        // We are not able to collect per field stats for stored, vector, points, and norms
        IndexDiskUsageStats.PerFieldDiskUsage actualTotal = actualStats.total();
        IndexDiskUsageStats.PerFieldDiskUsage expectedTotal = perFieldStats.total();
        assertFieldStats("total", "stored fields", actualTotal.getStoredFieldBytes(), expectedTotal.getStoredFieldBytes(), 0.01, 2048);
        assertFieldStats("total", "points", actualTotal.getPointsBytes(), expectedTotal.getPointsBytes(), 0.01, 2048);
        assertFieldStats("total", "term vectors", actualTotal.getTermVectorsBytes(), expectedTotal.getTermVectorsBytes(), 0.01, 2048);
        assertFieldStats("total", "norms", actualTotal.getNormsBytes(), expectedTotal.getNormsBytes(), 0.01, 2048);
    }

    private static void assertFieldStats(
        String fieldName,
        String fieldType,
        long actualBytes,
        long expectedBytes,
        double allowErrorPercentage,
        long allowErrorBytes
    ) {
        long margin = allowErrorBytes;
        if (allowErrorPercentage * actualBytes > allowErrorBytes) {
            margin = (long) (allowErrorPercentage * actualBytes);
        }
        final boolean inRange = expectedBytes - margin <= actualBytes && actualBytes <= expectedBytes + margin;
        if (inRange == false) {
            throw new AssertionError("field=" + fieldName + " type=" + fieldType + " actual=" + actualBytes + " expected=" + expectedBytes);
        }
    }

    private static IndexCommit lastCommit(Directory directory) throws IOException {
        final List<IndexCommit> commits = DirectoryReader.listCommits(directory);
        assertThat(commits, not(empty()));
        return commits.get(commits.size() - 1);
    }

    private static ShardId testShardId() {
        return new ShardId("test_index", "_na_", randomIntBetween(0, 3));
    }

    private static class RandomMatchQuery extends Query {
        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ConstantScoreWeight(this, 1.0f) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    final FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
                    for (int i = 0; i < bits.length(); i++) {
                        if (randomBoolean()) {
                            bits.set(i);
                        }
                    }
                    Scorer scorer = new ConstantScoreScorer(1.0f, ScoreMode.COMPLETE_NO_SCORES, new BitSetIterator(bits, bits.length()));
                    return new DefaultScorerSupplier(scorer);
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }
            };
        }

        @Override
        public String toString(String field) {
            return "RandomMatchQuery";
        }

        @Override
        public void visit(QueryVisitor visitor) {

        }

        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }
    }

    /**
     * Asserts that we properly handle situations where segments have FieldInfos, but associated documents are gone
     */
    private static void analyzeDiskUsageAfterDeleteRandomDocuments(Directory dir) throws IOException {
        int iterations = between(5, 20);
        for (int i = 0; i < iterations; i++) {
            IndexWriterConfig config = new IndexWriterConfig().setCommitOnClose(true);
            final IndexWriter.DocStats docStats;
            try (IndexWriter writer = new IndexWriter(dir, config); DirectoryReader reader = DirectoryReader.open(writer)) {
                writer.deleteDocuments(new RandomMatchQuery());
                writer.flush();
                writer.commit();
                docStats = writer.getDocStats();
            }
            IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            if (docStats.numDocs == 0) {
                return;
            }
            assertThat(stats.total().getPointsBytes(), greaterThanOrEqualTo(0L));
        }
    }
}
