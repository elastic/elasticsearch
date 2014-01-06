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
import org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat;
import org.apache.lucene.codecs.diskdv.DiskDocValuesFormat;
import org.apache.lucene.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.codecs.lucene41.Lucene41Codec;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;
import org.apache.lucene.codecs.lucene45.Lucene45Codec;
import org.apache.lucene.codecs.lucene45.Lucene45DocValuesFormat;
import org.apache.lucene.codecs.lucene46.Lucene46Codec;
import org.apache.lucene.codecs.memory.DirectPostingsFormat;
import org.apache.lucene.codecs.memory.MemoryDocValuesFormat;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.codecs.pulsing.Pulsing41PostingsFormat;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.codec.docvaluesformat.*;
import org.elasticsearch.index.codec.postingsformat.*;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;
import org.elasticsearch.indices.fielddata.breaker.DummyCircuitBreakerService;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CodecTests extends ElasticsearchLuceneTestCase {
    
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
        assertThat(codecService.codec("default"), instanceOf(Lucene46Codec.class));
        assertThat(codecService.codec("Lucene46"), instanceOf(Lucene46Codec.class));
        assertThat(codecService.codec("Lucene45"), instanceOf(Lucene45Codec.class));
        assertThat(codecService.codec("Lucene40"), instanceOf(Lucene40Codec.class));
        assertThat(codecService.codec("Lucene41"), instanceOf(Lucene41Codec.class));
        assertThat(codecService.codec("Lucene42"), instanceOf(Lucene42Codec.class));
        assertThat(codecService.codec("SimpleText"), instanceOf(SimpleTextCodec.class));
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

        if (PostingFormats.luceneBloomFilter) {
            assertThat(postingsFormatService.get("bloom_pulsing").get(), instanceOf(BloomFilteringPostingsFormat.class));
        } else {
            assertThat(postingsFormatService.get("bloom_pulsing").get(), instanceOf(BloomFilterPostingsFormat.class));
        }

        assertThat(postingsFormatService.get("pulsing"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("pulsing").get(), instanceOf(Pulsing41PostingsFormat.class));
        assertThat(postingsFormatService.get("Pulsing41"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("Pulsing41").get(), instanceOf(Pulsing41PostingsFormat.class));

        assertThat(postingsFormatService.get("memory"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("memory").get(), instanceOf(MemoryPostingsFormat.class));
        assertThat(postingsFormatService.get("Memory"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("Memory").get(), instanceOf(MemoryPostingsFormat.class));

        assertThat(postingsFormatService.get("direct"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("direct").get(), instanceOf(DirectPostingsFormat.class));
        assertThat(postingsFormatService.get("Direct"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("Direct").get(), instanceOf(DirectPostingsFormat.class));
    }

    @Test
    public void testResolveDefaultDocValuesFormats() throws Exception {
        DocValuesFormatService docValuesFormatService = createCodecService().docValuesFormatService();

        for (String dvf : Arrays.asList("memory", "disk", "Disk", "default")) {
            assertThat(docValuesFormatService.get(dvf), instanceOf(PreBuiltDocValuesFormatProvider.class));
        }
        assertThat(docValuesFormatService.get("memory").get(), instanceOf(MemoryDocValuesFormat.class));
        assertThat(docValuesFormatService.get("disk").get(), instanceOf(DiskDocValuesFormat.class));
        assertThat(docValuesFormatService.get("Disk").get(), instanceOf(DiskDocValuesFormat.class));
        assertThat(docValuesFormatService.get("default").get(), instanceOf(Lucene45DocValuesFormat.class));
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
    public void testResolvePostingFormatsFromMapping_memory() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("postings_format", "memory").endObject()
                .startObject("field2").field("type", "string").field("postings_format", "my_format1").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.codec.postings_format.my_format1.type", "memory")
                .put("index.codec.postings_format.my_format1.pack_fst", true)
                .put("index.codec.postings_format.my_format1.acceptable_overhead_ratio", 0.3f)
                .build();
        CodecService codecService = createCodecService(indexSettings);
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider().get(), instanceOf(MemoryPostingsFormat.class));

        assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider(), instanceOf(MemoryPostingsFormatProvider.class));
        MemoryPostingsFormatProvider provider = (MemoryPostingsFormatProvider) documentMapper.mappers().name("field2").mapper().postingsFormatProvider();
        assertThat(provider.packFst(), equalTo(true));
        assertThat(provider.acceptableOverheadRatio(), equalTo(0.3f));
    }

    @Test
    public void testResolvePostingFormatsFromMapping_direct() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("postings_format", "direct").endObject()
                .startObject("field2").field("type", "string").field("postings_format", "my_format1").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.codec.postings_format.my_format1.type", "direct")
                .put("index.codec.postings_format.my_format1.min_skip_count", 16)
                .put("index.codec.postings_format.my_format1.low_freq_cutoff", 64)
                .build();
        CodecService codecService = createCodecService(indexSettings);
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider().get(), instanceOf(DirectPostingsFormat.class));

        assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider(), instanceOf(DirectPostingsFormatProvider.class));
        DirectPostingsFormatProvider provider = (DirectPostingsFormatProvider) documentMapper.mappers().name("field2").mapper().postingsFormatProvider();
        assertThat(provider.minSkipCount(), equalTo(16));
        assertThat(provider.lowFreqCutoff(), equalTo(64));
    }

    @Test
    public void testResolvePostingFormatsFromMapping_pulsing() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("postings_format", "pulsing").endObject()
                .startObject("field2").field("type", "string").field("postings_format", "my_format1").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.codec.postings_format.my_format1.type", "pulsing")
                .put("index.codec.postings_format.my_format1.freq_cut_off", 2)
                .put("index.codec.postings_format.my_format1.min_block_size", 32)
                .put("index.codec.postings_format.my_format1.max_block_size", 64)
                .build();
        CodecService codecService = createCodecService(indexSettings);
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider().get(), instanceOf(Pulsing41PostingsFormat.class));

        assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider(), instanceOf(PulsingPostingsFormatProvider.class));
        PulsingPostingsFormatProvider provider = (PulsingPostingsFormatProvider) documentMapper.mappers().name("field2").mapper().postingsFormatProvider();
        assertThat(provider.freqCutOff(), equalTo(2));
        assertThat(provider.minBlockSize(), equalTo(32));
        assertThat(provider.maxBlockSize(), equalTo(64));
    }

    @Test
    public void testResolvePostingFormatsFromMappingLuceneBloom() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("postings_format", "bloom_default").endObject()
                .startObject("field2").field("type", "string").field("postings_format", "bloom_pulsing").endObject()
                .startObject("field3").field("type", "string").field("postings_format", "my_format1").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.codec.postings_format.my_format1.type", "bloom_filter_lucene")
                .put("index.codec.postings_format.my_format1.desired_max_saturation", 0.2f)
                .put("index.codec.postings_format.my_format1.saturation_limit", 0.8f)
                .put("index.codec.postings_format.my_format1.delegate", "delegate1")
                .put("index.codec.postings_format.delegate1.type", "direct")
                .put("index.codec.postings_format.delegate1.min_skip_count", 16)
                .put("index.codec.postings_format.delegate1.low_freq_cutoff", 64)
                .build();
        CodecService codecService = createCodecService(indexSettings);
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        if (PostingFormats.luceneBloomFilter) {
            assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider().get(), instanceOf(BloomFilteringPostingsFormat.class));
        } else {
            assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider().get(), instanceOf(BloomFilterPostingsFormat.class));
        }

        assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        if (PostingFormats.luceneBloomFilter) {
            assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider().get(), instanceOf(BloomFilteringPostingsFormat.class));
        } else {
            assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider().get(), instanceOf(BloomFilterPostingsFormat.class));
        }

        assertThat(documentMapper.mappers().name("field3").mapper().postingsFormatProvider(), instanceOf(BloomFilterLucenePostingsFormatProvider.class));
        BloomFilterLucenePostingsFormatProvider provider = (BloomFilterLucenePostingsFormatProvider) documentMapper.mappers().name("field3").mapper().postingsFormatProvider();
        assertThat(provider.desiredMaxSaturation(), equalTo(0.2f));
        assertThat(provider.saturationLimit(), equalTo(0.8f));
        assertThat(provider.delegate(), instanceOf(DirectPostingsFormatProvider.class));
        DirectPostingsFormatProvider delegate = (DirectPostingsFormatProvider) provider.delegate();
        assertThat(delegate.minSkipCount(), equalTo(16));
        assertThat(delegate.lowFreqCutoff(), equalTo(64));
    }

    @Test
    public void testChangeUidPostingsFormat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_uid").field("postings_format", "memory").endObject()
                .endObject().endObject().string();

        CodecService codecService = createCodecService();
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.rootMapper(UidFieldMapper.class).postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(documentMapper.rootMapper(UidFieldMapper.class).postingsFormatProvider().get(), instanceOf(MemoryPostingsFormat.class));
    }

    @Test
    public void testChangeUidDocValuesFormat() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_uid").startObject("fielddata").field("format", "doc_values").endObject().field("doc_values_format", "disk").endObject()
                .endObject().endObject().string();

        CodecService codecService = createCodecService();
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.rootMapper(UidFieldMapper.class).hasDocValues(), equalTo(true));
        assertThat(documentMapper.rootMapper(UidFieldMapper.class).docValuesFormatProvider(), instanceOf(PreBuiltDocValuesFormatProvider.class));
        assertThat(documentMapper.rootMapper(UidFieldMapper.class).docValuesFormatProvider().get(), instanceOf(DiskDocValuesFormat.class));
    }

    @Test
    public void testChangeIdDocValuesFormat() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_id").startObject("fielddata").field("format", "doc_values").endObject().field("doc_values_format", "disk").endObject()
                .endObject().endObject().string();

        CodecService codecService = createCodecService();
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.rootMapper(IdFieldMapper.class).hasDocValues(), equalTo(true));
        assertThat(documentMapper.rootMapper(IdFieldMapper.class).docValuesFormatProvider(), instanceOf(PreBuiltDocValuesFormatProvider.class));
        assertThat(documentMapper.rootMapper(IdFieldMapper.class).docValuesFormatProvider().get(), instanceOf(DiskDocValuesFormat.class));
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
        assertThat(documentMapper.mappers().name("field1").mapper().docValuesFormatProvider().get(), instanceOf(Lucene45DocValuesFormat.class));

        assertThat(documentMapper.mappers().name("field2").mapper().docValuesFormatProvider(), instanceOf(DefaultDocValuesFormatProvider.class));
    }

    @Test
    public void testResolveDocValuesFormatsFromMapping_memory() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "integer").field("doc_values_format", "memory").endObject()
                .startObject("field2").field("type", "double").field("doc_values_format", "my_format1").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.codec.doc_values_format.my_format1.type", "memory")
                .build();
        CodecService codecService = createCodecService(indexSettings);
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().docValuesFormatProvider(), instanceOf(PreBuiltDocValuesFormatProvider.class));
        assertThat(documentMapper.mappers().name("field1").mapper().docValuesFormatProvider().get(), instanceOf(MemoryDocValuesFormat.class));

        assertThat(documentMapper.mappers().name("field2").mapper().docValuesFormatProvider(), instanceOf(MemoryDocValuesFormatProvider.class));
    }

    @Test
    public void testResolveDocValuesFormatsFromMapping_disk() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "integer").field("doc_values_format", "disk").endObject()
                .startObject("field2").field("type", "double").field("doc_values_format", "my_format1").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.codec.doc_values_format.my_format1.type", "disk")
                .build();
        CodecService codecService = createCodecService(indexSettings);
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().docValuesFormatProvider(), instanceOf(PreBuiltDocValuesFormatProvider.class));
        assertThat(documentMapper.mappers().name("field1").mapper().docValuesFormatProvider().get(), instanceOf(DiskDocValuesFormat.class));

        assertThat(documentMapper.mappers().name("field2").mapper().docValuesFormatProvider(), instanceOf(DiskDocValuesFormatProvider.class));
    }

    @Test
    public void testChangeVersionFormat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_version").field("doc_values_format", "disk").endObject()
                .endObject().endObject().string();

        CodecService codecService = createCodecService();
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.rootMapper(VersionFieldMapper.class).docValuesFormatProvider(), instanceOf(PreBuiltDocValuesFormatProvider.class));
        assertThat(documentMapper.rootMapper(VersionFieldMapper.class).docValuesFormatProvider().get(), instanceOf(DiskDocValuesFormat.class));
    }

    private static CodecService createCodecService() {
        return createCodecService(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    private static CodecService createCodecService(Settings settings) {
        Index index = new Index("test");
        Injector injector = new ModulesBuilder()
                .add(new SettingsModule(settings))
                .add(new IndexNameModule(index))
                .add(new IndexSettingsModule(index, settings))
                .add(new SimilarityModule(settings))
                .add(new CodecModule(settings))
                .add(new MapperServiceModule())
                .add(new AnalysisModule(settings))
                .add(new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(CircuitBreakerService.class).to(DummyCircuitBreakerService.class);
                    }
                })
                .createInjector();
        return injector.getInstance(CodecService.class);
    }

}
