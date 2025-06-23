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
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

@SuppressCodecs("*") // we test against default codec so never get a random one here!
public class CodecTests extends ESTestCase {

    public void testResolveDefaultCodecs() throws Exception {
        assumeTrue("Only when zstd_stored_fields feature flag is enabled", CodecService.ZSTD_STORED_FIELDS_FEATURE_FLAG);
        CodecService codecService = createCodecService();
        assertThat(codecService.codec("default"), instanceOf(PerFieldMapperCodec.class));
        assertThat(codecService.codec("default"), instanceOf(Elasticsearch900Lucene101Codec.class));
    }

    public void testDefault() throws Exception {
        assumeTrue("Only when zstd_stored_fields feature flag is enabled", CodecService.ZSTD_STORED_FIELDS_FEATURE_FLAG);
        Codec codec = createCodecService().codec("default");
        assertEquals(
            "Zstd814StoredFieldsFormat(compressionMode=ZSTD(level=1), chunkSize=14336, maxDocsPerChunk=128, blockShift=10)",
            codec.storedFieldsFormat().toString()
        );
    }

    public void testBestCompression() throws Exception {
        Codec codec = createCodecService().codec("best_compression");
        assertEquals(
            "Zstd814StoredFieldsFormat(compressionMode=ZSTD(level=3), chunkSize=245760, maxDocsPerChunk=2048, blockShift=10)",
            codec.storedFieldsFormat().toString()
        );
    }

    public void testLegacyDefault() throws Exception {
        Codec codec = createCodecService().codec("legacy_default");
        assertThat(codec.storedFieldsFormat(), Matchers.instanceOf(Lucene90StoredFieldsFormat.class));
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
        assertThat(codec.storedFieldsFormat(), Matchers.instanceOf(Lucene90StoredFieldsFormat.class));
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

    private CodecService createCodecService() throws IOException {
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        IndexSettings settings = IndexSettingsModule.newIndexSettings("_na", nodeSettings);
        SimilarityService similarityService = new SimilarityService(settings, null, Collections.emptyMap());
        IndexAnalyzers indexAnalyzers = createTestAnalysis(settings, nodeSettings).indexAnalyzers;
        MapperRegistry mapperRegistry = new MapperRegistry(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            MapperPlugin.NOOP_FIELD_FILTER
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
            settings.getMode().idFieldMapperWithoutFieldData(),
            ScriptCompiler.NONE,
            bitsetFilterCache::getBitSetProducer,
            MapperMetrics.NOOP
        );
        return new CodecService(service, BigArrays.NON_RECYCLING_INSTANCE);
    }

}
