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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.codecs.lucene41.Lucene41Codec;
import org.apache.lucene.codecs.lucene410.Lucene410Codec;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;
import org.apache.lucene.codecs.lucene45.Lucene45Codec;
import org.apache.lucene.codecs.lucene46.Lucene46Codec;
import org.apache.lucene.codecs.lucene49.Lucene49Codec;
import org.apache.lucene.codecs.lucene50.Lucene50Codec;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;

@SuppressCodecs("*") // we test against default codec so never get a random one here!
public class CodecTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testResolveDefaultCodecs() throws Exception {
        CodecService codecService = createCodecService();
        assertThat(codecService.codec("default"), instanceOf(PerFieldMappingPostingFormatCodec.class));
        assertThat(codecService.codec("default"), instanceOf(Lucene50Codec.class));
        assertThat(codecService.codec("Lucene410"), instanceOf(Lucene410Codec.class));
        assertThat(codecService.codec("Lucene49"), instanceOf(Lucene49Codec.class));
        assertThat(codecService.codec("Lucene46"), instanceOf(Lucene46Codec.class));
        assertThat(codecService.codec("Lucene45"), instanceOf(Lucene45Codec.class));
        assertThat(codecService.codec("Lucene40"), instanceOf(Lucene40Codec.class));
        assertThat(codecService.codec("Lucene41"), instanceOf(Lucene41Codec.class));
        assertThat(codecService.codec("Lucene42"), instanceOf(Lucene42Codec.class));
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

    private static CodecService createCodecService() {
        return createCodecService(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    private static CodecService createCodecService(Settings settings) {
        IndexService indexService = createIndex("test", settings);
        return indexService.injector().getInstance(CodecService.class);
    }

}
