/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat;
import org.elasticsearch.index.codec.storedfields.TSDBStoredFieldsFormat;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.FieldInfoCachingDirectory;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

@SuppressCodecs("*") // we test against default codec so never get a random one here!
public class CodecTests extends ESTestCase {

    public void testResolveDefaultCodecs() throws Exception {
        CodecService codecService = createCodecService();
        var codec = codecService.codec("default");
        // PerFieldMapperCodec already shares field infos, so CodecService uses it as-is rather than wrapping it.
        assertThat(codec, instanceOf(PerFieldMapperCodec.class));
        assertThat(codec.fieldInfosFormat(), instanceOf(ElasticsearchFieldInfosFormat.class));
        assertThat(((Elasticsearch96Codec) codec).delegate(), instanceOf(Lucene104Codec.class));
    }

    /**
     * {@link Elasticsearch96Codec} exists only to give {@link Lucene104Codec} an Elasticsearch name, so it must
     * write exactly what Lucene's codec writes. Every format has to match; {@code fieldInfosFormat} is the single deliberate exception,
     * and that one changes which objects a read produces, not the bytes.
     */
    public void testElasticsearch96CodecWritesWhatLuceneWrites() {
        for (Lucene104Codec.Mode mode : Lucene104Codec.Mode.values()) {
            Codec lucene = new Lucene104Codec(mode);
            Codec es = new Elasticsearch96Codec(mode);

            // Named per-field wrappers: the name is what lands in the segment.
            assertEquals(mode.toString(), lucene.postingsFormat().getName(), es.postingsFormat().getName());
            // The doc values wrapper is the Elasticsearch fork, which is interchangeable with Lucene's: see
            // XPerFieldDocValuesFormatDuelTests. Its name is neither SPI-registered nor written to a segment.
            assertThat(es.docValuesFormat(), instanceOf(XPerFieldDocValuesFormat.class));
            assertEquals(mode.toString(), lucene.knnVectorsFormat().getName(), es.knnVectorsFormat().getName());

            // Everything else is inherited from the delegate and must stay identical.
            assertEquals(mode.toString(), lucene.storedFieldsFormat().getClass(), effectiveStoredFieldsFormat(es).getClass());
            assertEquals(mode.toString(), lucene.termVectorsFormat().getClass(), es.termVectorsFormat().getClass());
            assertEquals(mode.toString(), lucene.normsFormat().getClass(), es.normsFormat().getClass());
            assertEquals(mode.toString(), lucene.segmentInfoFormat().getClass(), es.segmentInfoFormat().getClass());
            assertEquals(mode.toString(), lucene.liveDocsFormat().getClass(), es.liveDocsFormat().getClass());
            assertEquals(mode.toString(), lucene.compoundFormat().getClass(), es.compoundFormat().getClass());

            // The intended differences. Both change how a segment is produced or read back, not the bytes on disk:
            // adaptive points writes Lucene90 point files with data-driven leaf sizes and reads with Lucene's own reader.
            assertThat(es.fieldInfosFormat(), instanceOf(ElasticsearchFieldInfosFormat.class));
            assertSame(mode.toString(), Elasticsearch900AdaptivePointsFormat.INSTANCE, es.pointsFormat());
        }
    }

    /**
     * The same read path with the per-directory cache switched off, where {@link DeduplicatingFieldInfosFormat} applies instead: field
     * infos are still distinct objects per segment, but their names come from a node-wide intern. Before the default codec had an
     * Elasticsearch name it reached neither format, so segments shared nothing at all — this half of the fix is invisible to the test
     * above, which requires the flag.
     */
    /**
     * Adaptive points only changes how BKD leaves are sized while writing; the files are Lucene90 point files and the reader is
     * Lucene's own. Segments written with it therefore have to stay queryable, which is what would break if the writer ever
     * diverged from the format the reader expects.
     */
    public void testAdaptivePointsSegmentsStayQueryable() throws Exception {
        Codec codec = createCodecService().codec(CodecService.DEFAULT_CODEC);
        assertSame(Elasticsearch900AdaptivePointsFormat.INSTANCE, codec.pointsFormat());
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(codec))) {
                for (int i = 0; i < 200; i++) {
                    Document doc = new Document();
                    doc.add(new IntField("int_field", i, Field.Store.NO));
                    w.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                assertEquals(200, searcher.count(IntField.newRangeQuery("int_field", 0, 199)));
                assertEquals(10, searcher.count(IntField.newRangeQuery("int_field", 5, 14)));
            }
        }
    }

    public void testDefaultCodecInternsFieldNamesWithoutTheCache() throws Exception {
        assumeFalse("covers the path taken when the per-Directory cache is off", FieldInfoCachingDirectory.FEATURE_FLAG.isEnabled());
        Codec codec = createCodecService().codec(CodecService.DEFAULT_CODEC);
        try (Directory dir = newDirectory()) {
            try (
                IndexWriter w = new IndexWriter(
                    dir,
                    newIndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE).setUseCompoundFile(false)
                )
            ) {
                for (int i = 0; i < 3; i++) {
                    Document doc = new Document();
                    doc.add(new KeywordField("string_field", "abc" + i, Field.Store.YES));
                    doc.add(new IntField("int_field", i, Field.Store.YES));
                    w.addDocument(doc);
                    w.commit();
                }
            }
            SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
            assertThat("test needs more than one segment to say anything", sis.size(), Matchers.greaterThan(1));

            Map<String, String> firstSeen = new HashMap<>();
            for (SegmentCommitInfo sci : sis) {
                FieldInfos fis = sci.info.getCodec().fieldInfosFormat().read(dir, sci.info, "", IOContext.DEFAULT);
                for (FieldInfo fi : fis) {
                    String prior = firstSeen.putIfAbsent(fi.getName(), fi.getName());
                    if (prior != null) {
                        assertSame("field name [" + fi.getName() + "] was not interned across segments", prior, fi.getName());
                    }
                }
            }
        }
    }

    /**
     * The behavioural counterpart of {@link #testCodecsWeWriteWithStillDeduplicateFieldInfosWhenResolvedByName()}: writes several
     * segments with the default codec and reads their field infos back exactly as {@code SegmentCoreReaders} does — through
     * {@code segmentInfo.getCodec()}, the codec resolved from the name in the segment, not the instance we wrote with. The existing
     * coverage in {@code CachingFieldInfosFormatTests} builds a {@code CachingFieldInfosFormat} by hand, so it stayed green while the
     * default codec resolved to one that never reached that format at all.
     */
    public void testDefaultCodecSharesFieldInfosAcrossSegmentsOnTheReadPath() throws Exception {
        assumeTrue("requires the per-Directory FieldInfo cache", FieldInfoCachingDirectory.FEATURE_FLAG.isEnabled());
        Codec codec = createCodecService().codec(CodecService.DEFAULT_CODEC);
        try (Directory raw = newDirectory()) {
            FieldInfoCachingDirectory dir = new FieldInfoCachingDirectory(raw);
            try (
                IndexWriter w = new IndexWriter(
                    dir,
                    newIndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE).setUseCompoundFile(false)
                )
            ) {
                for (int i = 0; i < 3; i++) {
                    Document doc = new Document();
                    doc.add(new KeywordField("string_field", "abc" + i, Field.Store.YES));
                    doc.add(new IntField("int_field", i, Field.Store.YES));
                    w.addDocument(doc);
                    w.commit();
                }
            }
            // Compound files are off so that the field infos live directly in the directory; the cache keys off segmentInfo.dir
            // either way, this just keeps the test able to open the file itself.
            SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
            assertThat("test needs more than one segment to say anything", sis.size(), Matchers.greaterThan(1));

            Map<String, FieldInfo> firstSeen = new HashMap<>();
            for (SegmentCommitInfo sci : sis) {
                FieldInfos fis = sci.info.getCodec().fieldInfosFormat().read(dir, sci.info, "", IOContext.DEFAULT);
                for (FieldInfo fi : fis) {
                    if (fi.isSoftDeletesField()) {
                        continue; // dvGen can differ per segment
                    }
                    FieldInfo prior = firstSeen.putIfAbsent(fi.getName(), fi);
                    if (prior != null) {
                        assertSame("FieldInfo for [" + fi.getName() + "] was not shared across segments", prior, fi);
                    }
                }
            }
            assertThat("nothing was cached", dir.fieldInfoCacheSize(), Matchers.greaterThan(0));
        }
    }

    /**
     * The codec used to read a segment is resolved by the name recorded in that segment ({@code SegmentInfos.readCommit} calls
     * {@code Codec.forName}), so it is never the instance {@link CodecService} built for writing. A codec whose name resolves to one
     * that does not deduplicate field infos therefore gets no sharing on the read path, and every segment retains its own copies —
     * expensive for mappings with many fields, and invisible to any test that only exercises writing.
     */
    public void testCodecsWeWriteWithStillDeduplicateFieldInfosWhenResolvedByName() throws Exception {
        for (boolean syntheticId : new boolean[] { false, true }) {
            CodecService codecService = createCodecService(syntheticId);
            for (String name : new String[] {
                CodecService.DEFAULT_CODEC,
                CodecService.BEST_COMPRESSION_CODEC,
                CodecService.LEGACY_DEFAULT_CODEC,
                CodecService.LEGACY_BEST_COMPRESSION_CODEC }) {
                Codec writeCodec = codecService.codec(name);
                Codec readCodec = Codec.forName(writeCodec.getName());
                assertThat(
                    "codec ["
                        + name
                        + "] writes segments named ["
                        + writeCodec.getName()
                        + "], which resolves to ["
                        + readCodec.getClass().getName()
                        + "] on read",
                    readCodec.fieldInfosFormat(),
                    instanceOf(ElasticsearchFieldInfosFormat.class)
                );
            }
        }
    }

    /**
     * Every codec Elasticsearch writes with shares field infos across a shard's segments.
     */
    public void testEveryWritableCodecSharesFieldInfos() throws Exception {
        for (boolean syntheticId : new boolean[] { false, true }) {
            CodecService codecService = createCodecService(syntheticId);
            for (String name : new String[] {
                CodecService.DEFAULT_CODEC,
                CodecService.BEST_COMPRESSION_CODEC,
                CodecService.LEGACY_DEFAULT_CODEC,
                CodecService.LEGACY_BEST_COMPRESSION_CODEC }) {
                Codec codec = codecService.codec(name);
                assertThat(
                    "codec [" + name + "] (syntheticId=" + syntheticId + ")",
                    codec.fieldInfosFormat(),
                    instanceOf(ElasticsearchFieldInfosFormat.class)
                );
            }
        }
    }

    /**
     * The per-field fallbacks come from the Lucene codec rather than being restated, so a codec for a newer Lucene inherits them.
     * These are the formats that were previously named here explicitly.
     */
    public void testPerFieldFallbacksComeFromTheLuceneCodec() {
        Elasticsearch96Codec es = new Elasticsearch96Codec();
        Lucene104Codec lucene = new Lucene104Codec();
        for (String field : new String[] { "a", "_id", "vector" }) {
            // Distinct instances, so compare what actually lands in a segment: the class and the recorded name.
            assertEquals(field, lucene.getPostingsFormatForField(field).getClass(), es.getPostingsFormatForField(field).getClass());
            assertEquals(field, lucene.getPostingsFormatForField(field).getName(), es.getPostingsFormatForField(field).getName());
            assertEquals(field, lucene.getDocValuesFormatForField(field).getClass(), es.getDocValuesFormatForField(field).getClass());
            assertEquals(field, lucene.getDocValuesFormatForField(field).getName(), es.getDocValuesFormatForField(field).getName());
            assertEquals(field, lucene.getKnnVectorsFormatForField(field).getClass(), es.getKnnVectorsFormatForField(field).getClass());
            assertEquals(field, lucene.getKnnVectorsFormatForField(field).getName(), es.getKnnVectorsFormatForField(field).getName());
        }
        assertThat(es.getPostingsFormatForField("a"), instanceOf(Lucene104PostingsFormat.class));
        assertThat(es.getDocValuesFormatForField("a"), instanceOf(Lucene90DocValuesFormat.class));
        assertThat(es.getKnnVectorsFormatForField("a"), instanceOf(Lucene99HnswVectorsFormat.class));
    }

    public void testDefault() throws Exception {
        Codec codec = createCodecService().codec("default");
        Lucene90StoredFieldsFormat storedFieldsFormat = (Lucene90StoredFieldsFormat) effectiveStoredFieldsFormat(codec);
        var mode = getLucene90StoredFieldsFormatMode(storedFieldsFormat);
        assertEquals(Lucene90StoredFieldsFormat.Mode.BEST_SPEED, mode);
    }

    public void testTSDBDefault() throws Exception {
        // Synthetic ids no longer get a codec of their own: the same codec covers them, differing per field and in what it
        // validates. Both values run every time, since a randomBoolean() would only exercise one of them per seed.
        for (boolean syntheticIdEnabled : new boolean[] { false, true }) {
            Codec codec = createCodecService(syntheticIdEnabled).codec("default");
            String message = "syntheticId=" + syntheticIdEnabled;
            assertThat(message, codec, instanceOf(PerFieldMapperCodec.class));
            assertEquals(message, "Elasticsearch96", codec.getName());
            assertThat(message, codec.fieldInfosFormat(), instanceOf(ElasticsearchFieldInfosFormat.class));
        }
    }

    public void testBestCompression() throws Exception {
        Codec codec = createCodecService().codec("best_compression");
        assertEquals(
            "Zstd814StoredFieldsFormat(compressionMode=ZSTD(level=3), chunkSize=245760, maxDocsPerChunk=2048, blockShift=10)",
            effectiveStoredFieldsFormat(codec).toString()
        );
    }

    public void testLegacyDefault() throws Exception {
        Codec codec = createCodecService().codec("legacy_default");
        assertThat(effectiveStoredFieldsFormat(codec), Matchers.instanceOf(Lucene90StoredFieldsFormat.class));
        // Make sure the legacy codec is writable
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(codec))) {
            Document doc = new Document();
            doc.add(new KeywordField("string_field", "abc", Field.Store.YES));
            doc.add(new IntField("int_field", 42, Field.Store.YES));
            w.addDocument(doc);
            try (DirectoryReader r = DirectoryReader.open(w)) {}
        }
    }

    public void testLegacyBestCompression() throws Exception {
        Codec codec = createCodecService().codec("legacy_best_compression");
        assertThat(effectiveStoredFieldsFormat(codec), Matchers.instanceOf(Lucene90StoredFieldsFormat.class));
        // Make sure the legacy codec is writable
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(codec))) {
            Document doc = new Document();
            doc.add(new KeywordField("string_field", "abc", Field.Store.YES));
            doc.add(new IntField("int_field", 42, Field.Store.YES));
            w.addDocument(doc);
            try (DirectoryReader r = DirectoryReader.open(w)) {}
        }
    }

    public void testCodecRetrievalForUnknownCodec() throws Exception {
        CodecService codecService = createCodecService();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> codecService.codec("unknown_codec"));
        assertEquals("failed to find codec [unknown_codec]", exception.getMessage());
    }

    public void testAvailableCodecsContainsExpectedCodecs() throws Exception {
        CodecService codecService = createCodecService();
        String[] availableCodecs = codecService.availableCodecs();
        List<String> codecList = Arrays.asList(availableCodecs);
        int expectedCodecCount = Codec.availableCodecs().size() + 5;

        assertTrue(codecList.contains(CodecService.DEFAULT_CODEC));
        assertTrue(codecList.contains(CodecService.LEGACY_DEFAULT_CODEC));
        assertTrue(codecList.contains(CodecService.BEST_COMPRESSION_CODEC));
        assertTrue(codecList.contains(CodecService.LEGACY_BEST_COMPRESSION_CODEC));
        assertTrue(codecList.contains(CodecService.LUCENE_DEFAULT_CODEC));

        assertFalse(codecList.contains("unknown_codec"));

        assertEquals(expectedCodecCount, availableCodecs.length);
    }

    /**
     * The implementation behind {@link ElasticsearchStoredFieldsFormat}, which selects it per segment.
     */
    private static StoredFieldsFormat effectiveStoredFieldsFormat(Codec codec) {
        StoredFieldsFormat format = codec.storedFieldsFormat();
        if (format instanceof TSDBStoredFieldsFormat tsdbFormat) {
            format = tsdbFormat.delegate();
        }
        return format instanceof ElasticsearchStoredFieldsFormat elasticsearchFormat ? elasticsearchFormat.writeFormat() : format;
    }

    private CodecService createCodecService() throws IOException {
        return createCodecService(false);
    }

    private CodecService createCodecService(boolean syntheticIdEnabled) throws IOException {
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        var indexSettings = Settings.builder().put(nodeSettings);
        if (syntheticIdEnabled) {
            indexSettings.put(IndexSettings.SYNTHETIC_ID.getKey(), true)
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "hostname");
        }
        IndexSettings settings = IndexSettingsModule.newIndexSettings("_na", indexSettings.build());
        SimilarityService similarityService = new SimilarityService(settings, null, Collections.emptyMap());
        IndexAnalyzers indexAnalyzers = createTestAnalysis(settings, nodeSettings).indexAnalyzers;
        MapperRegistry mapperRegistry = new MapperRegistry(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            MapperPlugin.NOOP_FIELD_FILTER,
            null
        );
        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(settings, BitsetFilterCache.Listener.NOOP);
        MapperService service = new MapperService(
            () -> TransportVersion.current(),
            settings,
            indexAnalyzers,
            parserConfig(),
            similarityService,
            mapperRegistry,
            () -> null,
            () -> false,
            ScriptCompiler.NONE,
            bitsetFilterCache::getBitSetProducer,
            MapperMetrics.NOOP,
            null,
            null
        );
        return new CodecService(service, BigArrays.NON_RECYCLING_INSTANCE, null);
    }

    @SuppressForbidden(reason = "access violation required in order to read private field for this test")
    static Lucene90StoredFieldsFormat.Mode getLucene90StoredFieldsFormatMode(Lucene90StoredFieldsFormat storedFieldsFormat)
        throws NoSuchFieldException, IllegalAccessException {
        var modeField = Lucene90StoredFieldsFormat.class.getDeclaredField("mode");
        modeField.setAccessible(true);
        return (Lucene90StoredFieldsFormat.Mode) modeField.get(storedFieldsFormat);
    }

}
