/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat;
import org.apache.lucene.codecs.lucene87.Lucene87Codec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.suggest.document.Completion84PostingsFormat;
import org.apache.lucene.search.suggest.document.CompletionPostingsFormat;
import org.apache.lucene.search.suggest.document.SuggestField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class IndexDiskUsageAnalyzerTests extends ESTestCase {

    public void testStoredFields() throws Exception {
        try (Directory dir = newDirectory()) {
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
            assertFieldStats("total", "stored field",
                stats.total().getStoredFieldBytes(), perField.total().getStoredFieldBytes(), 0.01, 1024);

            assertFieldStats("sf1", "stored field",
                stats.getFields().get("sf1").getStoredFieldBytes(), stats.total().getStoredFieldBytes() / 6, 0.01, 512);

            assertFieldStats("sf2", "stored field",
                stats.getFields().get("sf2").getStoredFieldBytes(), stats.total().getStoredFieldBytes() / 3, 0.01, 512);

            assertFieldStats("sf3", "stored field",
                stats.getFields().get("sf3").getStoredFieldBytes(), stats.total().getStoredFieldBytes() / 2, 0.01, 512);
        }
    }

    public void testTermVectors() throws Exception {
        try (Directory dir = newDirectory()) {
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
            assertFieldStats("total", "term vectors",
                stats.total().getTermVectorsBytes(), perField.total().getTermVectorsBytes(), 0.01, 1024);
            assertFieldStats("v1", "term vectors",
                stats.getFields().get("v1").getTermVectorsBytes(), stats.total().getTermVectorsBytes() / 7, 0.01, 512);
            assertFieldStats("v2", "term vectors",
                stats.getFields().get("v2").getTermVectorsBytes(), stats.total().getTermVectorsBytes() * 2 / 7, 0.01, 512);
            assertFieldStats("v3", "term vectors",
                stats.getFields().get("v3").getTermVectorsBytes(), stats.total().getTermVectorsBytes() * 4 / 7, 0.01, 512);
        }
    }

    public void testPoints() throws Exception {
        try (Directory dir = newDirectory()) {
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
            assertFieldStats("total", "points",
                stats.total().getPointsBytes(), perField.total().getPointsBytes(), 0.01, 1024);
            assertFieldStats("pt1", "points",
                stats.getFields().get("pt1").getPointsBytes(), stats.total().getPointsBytes() / 7, 0.01, 512);
            assertFieldStats("pt2", "points",
                stats.getFields().get("pt2").getPointsBytes(), stats.total().getPointsBytes() * 2 / 7, 0.01, 512);
            assertFieldStats("pt3", "points",
                stats.getFields().get("pt3").getPointsBytes(), stats.total().getPointsBytes() * 4 / 7, 0.01, 512);
        }
    }

    public void testCompletionField() throws Exception {
        IndexWriterConfig config = new IndexWriterConfig()
            .setCommitOnClose(true)
            .setUseCompoundFile(false)
            .setCodec(new Lucene87Codec(Lucene87Codec.Mode.BEST_SPEED) {
                @Override
                public PostingsFormat getPostingsFormatForField(String field) {
                    if (field.startsWith("suggest_")) {
                        return new Completion84PostingsFormat(randomFrom(CompletionPostingsFormat.FSTLoadMode.values()));
                    } else {
                        return super.postingsFormat();
                    }
                }
            });

        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                int numDocs = randomIntBetween(100, 1000);
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    if (randomDouble() < 0.5) {
                        doc.add(new SuggestField("suggest_1", randomAlphaOfLength(10), randomIntBetween(1, 20)));
                    }
                    doc.add(new SuggestField("suggest_2", randomAlphaOfLength(10), randomIntBetween(1, 20)));
                    writer.addDocument(doc);
                }
            }
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            assertFieldStats("suggest_1", "inverted_index",
                stats.getFields().get("suggest_1").getInvertedIndexBytes(),
                stats.total().totalBytes() / 3, 0.05, 2048);

            assertFieldStats("suggest_2", "inverted_index",
                stats.getFields().get("suggest_2").getInvertedIndexBytes(),
                stats.total().totalBytes() * 2 / 3, 0.05, 2048);

            final IndexDiskUsageStats perField = collectPerFieldStats(dir);
            assertFieldStats("suggest_1", "inverted_index",
                stats.getFields().get("suggest_1").getInvertedIndexBytes(),
                perField.getFields().get("suggest_1").getInvertedIndexBytes(), 0.05, 2048);

            assertFieldStats("suggest_2", "inverted_index",
                stats.getFields().get("suggest_2").getInvertedIndexBytes(),
                perField.getFields().get("suggest_2").getInvertedIndexBytes(), 0.05, 2048);
        }
    }

    public void testMixedFields() throws Exception {
        try (Directory dir = newDirectory()) {
            final CodecMode codec = randomFrom(CodecMode.values());
            indexRandomly(dir, codec, between(100, 1000), IndexDiskUsageAnalyzerTests::addRandomFields);
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(dir), () -> {});
            logger.info("--> stats {}", stats);
            try (Directory perFieldDir = newDirectory()) {
                rewriteIndexWithPerFieldCodec(dir, codec, perFieldDir);
                final IndexDiskUsageStats perFieldStats = collectPerFieldStats(perFieldDir);
                assertStats(stats, perFieldStats);
                assertStats(IndexDiskUsageAnalyzer.analyze(testShardId(), lastCommit(perFieldDir), () -> {}), perFieldStats);
            }
        }
    }

    enum CodecMode {
        BEST_SPEED {
            @Override
            Lucene87Codec.Mode mode() {
                return Lucene87Codec.Mode.BEST_SPEED;
            }

            @Override
            DocValuesFormat dvFormat() {
                return new Lucene80DocValuesFormat(Lucene80DocValuesFormat.Mode.BEST_SPEED);
            }
        },

        BEST_COMPRESSION {
            @Override
            Lucene87Codec.Mode mode() {
                return Lucene87Codec.Mode.BEST_COMPRESSION;
            }

            @Override
            DocValuesFormat dvFormat() {
                return new Lucene80DocValuesFormat(Lucene80DocValuesFormat.Mode.BEST_COMPRESSION);
            }
        };

        abstract Lucene87Codec.Mode mode();

        abstract DocValuesFormat dvFormat();
    }

    static void indexRandomly(Directory directory, CodecMode codecMode, int numDocs, Consumer<Document> addFields) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig()
            .setCommitOnClose(true)
            .setUseCompoundFile(randomBoolean())
            .setCodec(new Lucene87Codec(codecMode.mode()));
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < numDocs; i++) {
                final Document doc = new Document();
                addFields.accept(doc);
                writer.addDocument(doc);
            }
        }
    }

    static void addRandomDocValuesField(Document doc) {
        if (randomBoolean()) {
            doc.add(new NumericDocValuesField("ndv", random().nextInt(1024)));
        }
        if (randomBoolean()) {
            doc.add(new BinaryDocValuesField("bdv", new BytesRef(randomAlphaOfLength(3))));
        }
        if (randomBoolean()) {
            doc.add(new SortedDocValuesField("sdv", new BytesRef(randomAlphaOfLength(3))));
        }
        int numValues = random().nextInt(5);
        for (int i = 0; i < numValues; ++i) {
            doc.add(new SortedSetDocValuesField("ssdv", new BytesRef(randomAlphaOfLength(3))));
        }
        numValues = random().nextInt(5);
        for (int i = 0; i < numValues; ++i) {
            doc.add(new SortedNumericDocValuesField("sndv", random().nextInt(1024)));
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

    static void addRandomPoints(Document doc) {
        final int numValues = random().nextInt(5);
        for (int i = 0; i < numValues; i++) {
            doc.add(new IntPoint("pt-" + randomIntBetween(1, 2), random().nextInt()));
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

    static void addRandomFields(Document doc) {
        if (randomBoolean()) {
            addRandomDocValuesField(doc);
        }
        if (randomBoolean()) {
            addRandomPostings(doc);
        }
        if (randomBoolean()) {
            addRandomPoints(doc);
        }
        if (randomBoolean()) {
            addRandomStoredFields(doc, between(1, 3));
        }
        if (randomBoolean()) {
            addRandomTermVectors(doc);
        }
    }

    static class FieldLookup {
        private final Map<String, FieldInfo> dvSuffixes = new HashMap<>();
        private final Map<String, FieldInfo> postingsSuffixes = new HashMap<>();

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
    }

    static void rewriteIndexWithPerFieldCodec(Directory source, CodecMode mode, Directory dst) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(source)) {
            IndexWriterConfig config = new IndexWriterConfig()
                .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
                .setUseCompoundFile(randomBoolean())
                .setCodec(new Lucene87Codec(mode.mode()) {
                    @Override
                    public PostingsFormat getPostingsFormatForField(String field) {
                        return new Lucene84PostingsFormat();
                    }

                    @Override
                    public DocValuesFormat getDocValuesFormatForField(String field) {
                        return mode.dvFormat();
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
            directory = sis.getCodec().compoundFormat().getCompoundReader(reader.directory(), sis, IOContext.READ);
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
                    case DVD:
                    case DVM:
                        stats.addDocValues(fieldLookup.getDocValuesField(file), bytes);
                        break;
                    case TIM:
                    case TIP:
                    case TMD:
                    case DOC:
                    case POS:
                    case PAY:
                        stats.addInvertedIndex(fieldLookup.getPostingsField(file), bytes);
                        break;
                    case KDI:
                    case KDD:
                    case KDM:
                    case DIM:
                        stats.addPoints("_all_points_fields", bytes);
                        break;
                    case FDT:
                    case FDX:
                    case FDM:
                        // We don't have per field Codec for stored, vector, and norms field
                        stats.addStoredField("_all_stored_fields", bytes);
                        break;
                    case TVX:
                    case TVD:
                        stats.addTermVectors("_all_vectors_fields", bytes);
                        break;
                    case NVD:
                    case NVM:
                        stats.addNorms("_all_norms_fields", bytes);
                        break;
                    default:
                        break;
                }
            }
        } finally {
            if (directory != reader.directory()) {
                IOUtils.close(directory);
            }
        }
    }

    private static void assertStats(IndexDiskUsageStats actualStats, IndexDiskUsageStats perFieldStats) {
        final List<String> fields = actualStats.getFields().keySet().stream().sorted().collect(Collectors.toList());
        for (String field : fields) {
            IndexDiskUsageStats.PerFieldDiskUsage actualField = actualStats.getFields().get(field);
            IndexDiskUsageStats.PerFieldDiskUsage expectedField = perFieldStats.getFields().get(field);
            if (expectedField == null) {
                assertThat(actualField.getDocValuesBytes(), equalTo(0L));
                assertThat(actualField.getInvertedIndexBytes(), equalTo(0L));
                continue;
            }
            // Allow difference up to 2.5KB as we can load up to 256 long values in the table for numeric docValues
            assertFieldStats(field, "doc values",
                actualField.getDocValuesBytes(), expectedField.getDocValuesBytes(), 0.01, 2560);
            assertFieldStats(field, "inverted index",
                actualField.getInvertedIndexBytes(), expectedField.getInvertedIndexBytes(), 0.01, 1024);
        }
        // We are not able to collect per field stats for stored, vector, points, and norms
        IndexDiskUsageStats.PerFieldDiskUsage actualTotal = actualStats.total();
        IndexDiskUsageStats.PerFieldDiskUsage expectedTotal = perFieldStats.total();
        assertFieldStats("total", "stored fields", actualTotal.getStoredFieldBytes(), expectedTotal.getStoredFieldBytes(), 0.01, 1024);
        assertFieldStats("total", "points", actualTotal.getPointsBytes(), expectedTotal.getPointsBytes(), 0.01, 1024);
        assertFieldStats("total", "term vectors", actualTotal.getTermVectorsBytes(), expectedTotal.getTermVectorsBytes(), 0.01, 1024);
        assertFieldStats("total", "norms", actualTotal.getNormsBytes(), expectedTotal.getNormsBytes(), 0.01, 1024);
    }

    private static void assertFieldStats(String fieldName, String fieldType,
                                         long actualBytes, long expectedBytes,
                                         double allowErrorPercentage, long allowErrorBytes) {
        long margin = allowErrorBytes;
        if (allowErrorPercentage * actualBytes > allowErrorBytes) {
            margin = (long) (allowErrorPercentage * actualBytes);
        }
        final boolean inRange = expectedBytes - margin <= actualBytes && actualBytes <= expectedBytes + margin;
        if (inRange == false) {
            throw new AssertionError(
                "field=" + fieldName + " type=" + fieldType + " actual=" + actualBytes + " expected=" + expectedBytes);
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
}
