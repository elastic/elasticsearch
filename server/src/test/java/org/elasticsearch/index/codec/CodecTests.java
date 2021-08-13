/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.codecs.lucene87.Lucene87Codec;
import org.apache.lucene.codecs.lucene87.Lucene87StoredFieldsFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.instanceOf;

@SuppressCodecs("*") // we test against default codec so never get a random one here!
public class CodecTests extends ESTestCase {

    public void testResolveDefaultCodecs() throws Exception {
        CodecService codecService = createCodecService();
        assertThat(codecService.codec("default"), instanceOf(PerFieldMappingPostingFormatCodec.class));
        assertThat(codecService.codec("default"), instanceOf(Lucene87Codec.class));
        assertThat(codecService.codec("Lucene87"), instanceOf(Lucene87Codec.class));
    }

    public void testDefault() throws Exception {
        Codec codec = createCodecService().codec("default");
        assertStoredFieldsCompressionEquals(Lucene87StoredFieldsFormat.Mode.BEST_SPEED, codec);
        assertDocValuesCompressionEquals(Lucene80DocValuesFormat.Mode.BEST_COMPRESSION, codec);
    }

    public void testBestCompression() throws Exception {
        Codec codec = createCodecService().codec("best_compression");
        assertStoredFieldsCompressionEquals(Lucene87StoredFieldsFormat.Mode.BEST_COMPRESSION, codec);
        assertDocValuesCompressionEquals(Lucene80DocValuesFormat.Mode.BEST_COMPRESSION, codec);
    }

    // write some docs with it, inspect .si to see this was the used compression
    private void assertStoredFieldsCompressionEquals(Lucene87StoredFieldsFormat.Mode expected, Codec actual) throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(null);
        iwc.setCodec(actual);
        IndexWriter iw = new IndexWriter(dir, iwc);
        iw.addDocument(new Document());
        iw.commit();
        iw.close();
        DirectoryReader ir = DirectoryReader.open(dir);
        SegmentReader sr = (SegmentReader) ir.leaves().get(0).reader();
        String v = sr.getSegmentInfo().info.getAttribute(Lucene87StoredFieldsFormat.MODE_KEY);
        assertNotNull(v);
        assertEquals(expected, Lucene87StoredFieldsFormat.Mode.valueOf(v));
        ir.close();
        dir.close();
    }

    // write some docs with it, inspect .si to see this was the used compression
    private void assertDocValuesCompressionEquals(Lucene80DocValuesFormat.Mode expected, Codec actual) throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(null);
        iwc.setCodec(actual);
        IndexWriter iw = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new BinaryDocValuesField("foo", new BytesRef("aaa")));
        iw.addDocument(doc);
        iw.commit();
        iw.close();
        DirectoryReader ir = DirectoryReader.open(dir);
        SegmentReader sr = (SegmentReader) ir.leaves().get(0).reader();
        String v = sr.getFieldInfos().fieldInfo("foo").getAttribute(Lucene80DocValuesFormat.MODE_KEY);
        assertNotNull(v);
        assertEquals(expected, Lucene80DocValuesFormat.Mode.valueOf(v));
        ir.close();
        dir.close();
    }

    private CodecService createCodecService() throws IOException {
        Settings nodeSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .build();
        IndexSettings settings = IndexSettingsModule.newIndexSettings("_na", nodeSettings);
        SimilarityService similarityService = new SimilarityService(settings, null, Collections.emptyMap());
        IndexAnalyzers indexAnalyzers = createTestAnalysis(settings, nodeSettings).indexAnalyzers;
        MapperRegistry mapperRegistry = new MapperRegistry(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
            MapperPlugin.NOOP_FIELD_FILTER);
        MapperService service = new MapperService(settings, indexAnalyzers, xContentRegistry(), similarityService, mapperRegistry,
                () -> null, () -> false, ScriptCompiler.NONE);
        return new CodecService(service, LogManager.getLogger("test"));
    }

}
