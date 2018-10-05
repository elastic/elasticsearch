/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene.codecs.lucene80.Lucene80Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.MapperPlugin;
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
        assertThat(codecService.codec("default"), instanceOf(Lucene80Codec.class));
        assertThat(codecService.codec("Lucene80"), instanceOf(Lucene80Codec.class));
    }

    public void testDefault() throws Exception {
        Codec codec = createCodecService().codec("default");
        assertCompressionEquals(Mode.BEST_SPEED, codec);
    }

    public void testBestCompression() throws Exception {
        Codec codec = createCodecService().codec("best_compression");
        assertCompressionEquals(Mode.BEST_COMPRESSION, codec);
    }

    // write some docs with it, inspect .si to see this was the used compression
    private void assertCompressionEquals(Mode expected, Codec actual) throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(null);
        iwc.setCodec(actual);
        IndexWriter iw = new IndexWriter(dir, iwc);
        iw.addDocument(new Document());
        iw.commit();
        iw.close();
        DirectoryReader ir = DirectoryReader.open(dir);
        SegmentReader sr = (SegmentReader) ir.leaves().get(0).reader();
        String v = sr.getSegmentInfo().info.getAttribute(Lucene50StoredFieldsFormat.MODE_KEY);
        assertNotNull(v);
        assertEquals(expected, Mode.valueOf(v));
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
        MapperRegistry mapperRegistry = new MapperRegistry(Collections.emptyMap(), Collections.emptyMap(), MapperPlugin.NOOP_FIELD_FILTER);
        MapperService service = new MapperService(settings, indexAnalyzers, xContentRegistry(), similarityService, mapperRegistry,
                () -> null);
        return new CodecService(service, LogManager.getLogger("test"));
    }

}
