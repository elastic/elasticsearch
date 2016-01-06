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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.plain.PagedBytesAtomicFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericDVIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetDVOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.MapperBuilders;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.mapper.core.ByteFieldMapper;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.FloatFieldMapper;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.ShortFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class IndexFieldDataServiceTests extends ESSingleNodeTestCase {

    public void testGetForFieldDefaults() {
        final IndexService indexService = createIndex("test");
        final IndexFieldDataService ifdService = indexService.fieldData();
        final BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        final MappedFieldType stringMapper = new StringFieldMapper.Builder("string").tokenized(false).build(ctx).fieldType();
        ifdService.clear();
        IndexFieldData<?> fd = ifdService.getForField(stringMapper);
        assertTrue(fd instanceof SortedSetDVOrdinalsIndexFieldData);

        for (MappedFieldType mapper : Arrays.asList(
                new ByteFieldMapper.Builder("int").build(ctx).fieldType(),
                new ShortFieldMapper.Builder("int").build(ctx).fieldType(),
                new IntegerFieldMapper.Builder("int").build(ctx).fieldType(),
                new LongFieldMapper.Builder("long").build(ctx).fieldType()
                )) {
            ifdService.clear();
            fd = ifdService.getForField(mapper);
            assertTrue(fd instanceof SortedNumericDVIndexFieldData);
        }

        final MappedFieldType floatMapper = new FloatFieldMapper.Builder("float").build(ctx).fieldType();
        ifdService.clear();
        fd = ifdService.getForField(floatMapper);
        assertTrue(fd instanceof SortedNumericDVIndexFieldData);

        final MappedFieldType doubleMapper = new DoubleFieldMapper.Builder("double").build(ctx).fieldType();
        ifdService.clear();
        fd = ifdService.getForField(doubleMapper);
        assertTrue(fd instanceof SortedNumericDVIndexFieldData);
    }

    public void testChangeFieldDataFormat() throws Exception {
        final IndexService indexService = createIndex("test");
        final IndexFieldDataService ifdService = indexService.fieldData();
        final BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        final MappedFieldType mapper1 = MapperBuilders.stringField("s").tokenized(false).docValues(true).fieldDataSettings(Settings.builder().put(FieldDataType.FORMAT_KEY, "paged_bytes").build()).build(ctx).fieldType();
        final IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField("s", "thisisastring", Store.NO));
        writer.addDocument(doc);
        final IndexReader reader1 = DirectoryReader.open(writer, true);
        IndexFieldData<?> ifd = ifdService.getForField(mapper1);
        assertThat(ifd, instanceOf(PagedBytesIndexFieldData.class));
        Set<LeafReader> oldSegments = Collections.newSetFromMap(new IdentityHashMap<LeafReader, Boolean>());
        for (LeafReaderContext arc : reader1.leaves()) {
            oldSegments.add(arc.reader());
            AtomicFieldData afd = ifd.load(arc);
            assertThat(afd, instanceOf(PagedBytesAtomicFieldData.class));
        }
        // write new segment
        writer.addDocument(doc);
        final IndexReader reader2 = DirectoryReader.open(writer, true);
        final MappedFieldType mapper2 = MapperBuilders.stringField("s").tokenized(false).docValues(true).fieldDataSettings(Settings.builder().put(FieldDataType.FORMAT_KEY, "doc_values").build()).build(ctx).fieldType();
        ifd = ifdService.getForField(mapper2);
        assertThat(ifd, instanceOf(SortedSetDVOrdinalsIndexFieldData.class));
        reader1.close();
        reader2.close();
        writer.close();
        writer.getDirectory().close();
    }

    public void testFieldDataCacheListener() throws Exception {
        final IndexService indexService = createIndex("test");
        IndexFieldDataService shardPrivateService = indexService.fieldData();
        // copy the ifdService since we can set the listener only once.
        final IndexFieldDataService ifdService = new IndexFieldDataService(indexService.getIndexSettings(),
                getInstanceFromNode(IndicesFieldDataCache.class), getInstanceFromNode(CircuitBreakerService.class), indexService.mapperService());

        final BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        final MappedFieldType mapper1 = MapperBuilders.stringField("s").tokenized(false).docValues(true).fieldDataSettings(Settings.builder().put(FieldDataType.FORMAT_KEY, "paged_bytes").build()).build(ctx).fieldType();
        final IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField("s", "thisisastring", Store.NO));
        writer.addDocument(doc);
        DirectoryReader open = DirectoryReader.open(writer, true);
        final boolean wrap = randomBoolean();
        final IndexReader reader = wrap ? ElasticsearchDirectoryReader.wrap(open, new ShardId("test", 1)) : open;
        final AtomicInteger onCacheCalled = new AtomicInteger();
        final AtomicInteger onRemovalCalled = new AtomicInteger();
        ifdService.setListener(new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, FieldDataType fieldDataType, Accountable ramUsage) {
                if (wrap) {
                    assertEquals(new ShardId("test", 1), shardId);
                } else {
                    assertNull(shardId);
                }
                onCacheCalled.incrementAndGet();
            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, FieldDataType fieldDataType, boolean wasEvicted, long sizeInBytes) {
                if (wrap) {
                    assertEquals(new ShardId("test", 1), shardId);
                } else {
                    assertNull(shardId);
                }
                onRemovalCalled.incrementAndGet();
            }
        });
        IndexFieldData<?> ifd = ifdService.getForField(mapper1);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        AtomicFieldData load = ifd.load(leafReaderContext);
        assertEquals(1, onCacheCalled.get());
        assertEquals(0, onRemovalCalled.get());
        reader.close();
        load.close();
        writer.close();
        assertEquals(1, onCacheCalled.get());
        assertEquals(1, onRemovalCalled.get());
        ifdService.clear();
    }

    public void testSetCacheListenerTwice() {
        final IndexService indexService = createIndex("test");
        IndexFieldDataService shardPrivateService = indexService.fieldData();
        try {
            shardPrivateService.setListener(new IndexFieldDataCache.Listener() {
                @Override
                public void onCache(ShardId shardId, String fieldName, FieldDataType fieldDataType, Accountable ramUsage) {

                }

                @Override
                public void onRemoval(ShardId shardId, String fieldName, FieldDataType fieldDataType, boolean wasEvicted, long sizeInBytes) {

                }
            });
            fail("listener already set");
        } catch (IllegalStateException ex) {
            // all well
        }
    }

    private void doTestRequireDocValues(MappedFieldType ft) {
        ThreadPool threadPool = new ThreadPool("random_threadpool_name");
        try {
            IndicesFieldDataCache cache = new IndicesFieldDataCache(Settings.EMPTY, null, threadPool);
            IndexFieldDataService ifds = new IndexFieldDataService(IndexSettingsModule.newIndexSettings(new Index("test"), Settings.EMPTY), cache, null, null);
            ft.setName("some_long");
            ft.setHasDocValues(true);
            ifds.getForField(ft); // no exception
            ft.setHasDocValues(false);
            try {
                ifds.getForField(ft);
                fail();
            } catch (IllegalStateException e) {
                assertThat(e.getMessage(), containsString("doc values"));
            }
        } finally {
            threadPool.shutdown();
        }
    }

    public void testRequireDocValuesOnLongs() {
        doTestRequireDocValues(new LongFieldMapper.LongFieldType());
    }

    public void testRequireDocValuesOnDoubles() {
        doTestRequireDocValues(new DoubleFieldMapper.DoubleFieldType());
    }

    public void testRequireDocValuesOnBools() {
        doTestRequireDocValues(new BooleanFieldMapper.BooleanFieldType());
    }
}
