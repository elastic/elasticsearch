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

import java.util.Arrays;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.codecs.lucene41.Lucene41Codec;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;
import org.apache.lucene.codecs.lucene45.Lucene45Codec;
import org.apache.lucene.codecs.lucene46.Lucene46Codec;
import org.apache.lucene.codecs.lucene49.Lucene49Codec;
import org.apache.lucene.codecs.lucene410.Lucene410Codec;
import org.apache.lucene.codecs.lucene410.Lucene410DocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.codec.docvaluesformat.*;
import org.elasticsearch.index.codec.postingsformat.*;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.test.ElasticsearchSingleNodeLuceneTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CodecTests extends ElasticsearchSingleNodeLuceneTestCase {
    
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        forceDefaultCodec(); // we test against default codec so never get a random one here!
    }

    @Test
    public void testResolveDefaultCodecs() throws Exception {
        CodecService codecService = createCodecService();
        assertThat(codecService.codec("default"), instanceOf(PerFieldMappingPostingFormatCodec.class));
        assertThat(codecService.codec("default"), instanceOf(Lucene410Codec.class));
        assertThat(codecService.codec("Lucene410"), instanceOf(Lucene410Codec.class));
        assertThat(codecService.codec("Lucene49"), instanceOf(Lucene49Codec.class));
        assertThat(codecService.codec("Lucene46"), instanceOf(Lucene46Codec.class));
        assertThat(codecService.codec("Lucene45"), instanceOf(Lucene45Codec.class));
        assertThat(codecService.codec("Lucene40"), instanceOf(Lucene40Codec.class));
        assertThat(codecService.codec("Lucene41"), instanceOf(Lucene41Codec.class));
        assertThat(codecService.codec("Lucene42"), instanceOf(Lucene42Codec.class));
    }

    @Test
    public void testResolveDefaultPostingFormats() throws Exception {
        PostingsFormatService postingsFormatService = createCodecService().postingsFormatService();
        assertThat(postingsFormatService.get("default"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("default").get(), instanceOf(Elasticsearch090PostingsFormat.class));

        // Should fail when upgrading Lucene with codec changes
        assertThat(((Elasticsearch090PostingsFormat)postingsFormatService.get("default").get()).getDefaultWrapped(), instanceOf(((PerFieldPostingsFormat) Codec.getDefault().postingsFormat()).getPostingsFormatForField("").getClass()));
        assertThat(postingsFormatService.get("Lucene41"), instanceOf(PreBuiltPostingsFormatProvider.class));
        // Should fail when upgrading Lucene with codec changes
        assertThat(postingsFormatService.get("Lucene41").get(), instanceOf(((PerFieldPostingsFormat) Codec.getDefault().postingsFormat()).getPostingsFormatForField(null).getClass()));

        assertThat(postingsFormatService.get("bloom_default"), instanceOf(PreBuiltPostingsFormatProvider.class));
        if (PostingFormats.luceneBloomFilter) {
            assertThat(postingsFormatService.get("bloom_default").get(), instanceOf(BloomFilteringPostingsFormat.class));
        } else {
            assertThat(postingsFormatService.get("bloom_default").get(), instanceOf(BloomFilterPostingsFormat.class));
        }
        assertThat(postingsFormatService.get("BloomFilter"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("BloomFilter").get(), instanceOf(BloomFilteringPostingsFormat.class));

        assertThat(postingsFormatService.get("XBloomFilter"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("XBloomFilter").get(), instanceOf(BloomFilterPostingsFormat.class));
    }

    @Test
    public void testResolveDefaultDocValuesFormats() throws Exception {
        DocValuesFormatService docValuesFormatService = createCodecService().docValuesFormatService();

        for (String dvf : Arrays.asList("default")) {
            assertThat(docValuesFormatService.get(dvf), instanceOf(PreBuiltDocValuesFormatProvider.class));
        }
        assertThat(docValuesFormatService.get("default").get(), instanceOf(Lucene410DocValuesFormat.class));
    }

    @Test
    public void testResolvePostingFormatsFromMapping_default() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("postings_format", "default").endObject()
                .startObject("field2").field("type", "string").field("postings_format", "my_format1").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.codec.postings_format.my_format1.type", "default")
                .put("index.codec.postings_format.my_format1.min_block_size", 16)
                .put("index.codec.postings_format.my_format1.max_block_size", 64)
                .build();
        CodecService codecService = createCodecService(indexSettings);
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider().get(), instanceOf(Elasticsearch090PostingsFormat.class));

        assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider(), instanceOf(DefaultPostingsFormatProvider.class));
        DefaultPostingsFormatProvider provider = (DefaultPostingsFormatProvider) documentMapper.mappers().name("field2").mapper().postingsFormatProvider();
        assertThat(provider.minBlockSize(), equalTo(16));
        assertThat(provider.maxBlockSize(), equalTo(64));
    }

    @Test
    public void testChangeUidPostingsFormat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_uid").field("postings_format", "Lucene41").endObject()
                .endObject().endObject().string();

        CodecService codecService = createCodecService();
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.rootMapper(UidFieldMapper.class).postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(documentMapper.rootMapper(UidFieldMapper.class).postingsFormatProvider().get(), instanceOf(Lucene41PostingsFormat.class));
    }

    @Test
    public void testResolveDocValuesFormatsFromMapping_default() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "integer").field("doc_values_format", "default").endObject()
                .startObject("field2").field("type", "double").field("doc_values_format", "my_format1").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.codec.doc_values_format.my_format1.type", "default")
                .build();
        CodecService codecService = createCodecService(indexSettings);
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().docValuesFormatProvider(), instanceOf(PreBuiltDocValuesFormatProvider.class));
        assertThat(documentMapper.mappers().name("field1").mapper().docValuesFormatProvider().get(), instanceOf(Lucene410DocValuesFormat.class));

        assertThat(documentMapper.mappers().name("field2").mapper().docValuesFormatProvider(), instanceOf(DefaultDocValuesFormatProvider.class));
    }

    @Test
    public void testChangeVersionFormat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_version").field("doc_values_format", "Lucene410").endObject()
                .endObject().endObject().string();

        CodecService codecService = createCodecService();
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.rootMapper(VersionFieldMapper.class).docValuesFormatProvider(), instanceOf(PreBuiltDocValuesFormatProvider.class));
        assertThat(documentMapper.rootMapper(VersionFieldMapper.class).docValuesFormatProvider().get(), instanceOf(Lucene410DocValuesFormat.class));
    }

    private static CodecService createCodecService() {
        return createCodecService(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    private static CodecService createCodecService(Settings settings) {
        IndexService indexService = createIndex("test", settings);
        return indexService.injector().getInstance(CodecService.class);
    }

}
