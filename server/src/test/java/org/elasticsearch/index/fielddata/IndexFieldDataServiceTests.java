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
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.plain.SortedNumericDVIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetDVOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.JsonFieldMapper;
import org.elasticsearch.index.mapper.JsonFieldMapper.KeyedJsonIndexFieldData;
import org.elasticsearch.index.mapper.JsonFieldMapper.KeyedJsonFieldType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;

public class IndexFieldDataServiceTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testGetForFieldDefaults() {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexFieldDataService ifdService = new IndexFieldDataService(indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(), indicesService.getCircuitBreakerService(), indexService.mapperService());
        final BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        final MappedFieldType stringMapper = new KeywordFieldMapper.Builder("string").build(ctx).fieldType();
        ifdService.clear();
        IndexFieldData<?> fd = ifdService.getForField(stringMapper);
        assertTrue(fd instanceof SortedSetDVOrdinalsIndexFieldData);

        for (MappedFieldType mapper : Arrays.asList(
                new NumberFieldMapper.Builder("int", NumberFieldMapper.NumberType.BYTE).build(ctx).fieldType(),
                new NumberFieldMapper.Builder("int", NumberFieldMapper.NumberType.SHORT).build(ctx).fieldType(),
                new NumberFieldMapper.Builder("int", NumberFieldMapper.NumberType.INTEGER).build(ctx).fieldType(),
                new NumberFieldMapper.Builder("long", NumberFieldMapper.NumberType.LONG).build(ctx).fieldType()
                )) {
            ifdService.clear();
            fd = ifdService.getForField(mapper);
            assertTrue(fd instanceof SortedNumericDVIndexFieldData);
        }

        final MappedFieldType floatMapper = new NumberFieldMapper.Builder("float", NumberFieldMapper.NumberType.FLOAT)
                .build(ctx).fieldType();
        ifdService.clear();
        fd = ifdService.getForField(floatMapper);
        assertTrue(fd instanceof SortedNumericDVIndexFieldData);

        final MappedFieldType doubleMapper = new NumberFieldMapper.Builder("double", NumberFieldMapper.NumberType.DOUBLE)
                .build(ctx).fieldType();
        ifdService.clear();
        fd = ifdService.getForField(doubleMapper);
        assertTrue(fd instanceof SortedNumericDVIndexFieldData);
    }

    public void testClearField() throws Exception {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        // copy the ifdService since we can set the listener only once.
        final IndexFieldDataService ifdService = new IndexFieldDataService(indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(), indicesService.getCircuitBreakerService(), indexService.mapperService());

        final BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        final MappedFieldType mapper1 = new TextFieldMapper.Builder("field_1").fielddata(true).build(ctx).fieldType();
        final MappedFieldType mapper2 = new TextFieldMapper.Builder("field_2").fielddata(true).build(ctx).fieldType();
        final IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField("field_1", "thisisastring", Store.NO));
        doc.add(new StringField("field_2", "thisisanotherstring", Store.NO));
        writer.addDocument(doc);
        final IndexReader reader = DirectoryReader.open(writer);
        final AtomicInteger onCacheCalled = new AtomicInteger();
        final AtomicInteger onRemovalCalled = new AtomicInteger();
        ifdService.setListener(new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
                onCacheCalled.incrementAndGet();
            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                onRemovalCalled.incrementAndGet();
            }
        });
        IndexFieldData<?> ifd1 = ifdService.getForField(mapper1);
        IndexFieldData<?> ifd2 = ifdService.getForField(mapper2);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        AtomicFieldData loadField1 = ifd1.load(leafReaderContext);
        AtomicFieldData loadField2 = ifd2.load(leafReaderContext);

        assertEquals(2, onCacheCalled.get());
        assertEquals(0, onRemovalCalled.get());

        ifdService.clearField("field_1");

        assertEquals(2, onCacheCalled.get());
        assertEquals(1, onRemovalCalled.get());

        ifdService.clearField("field_1");

        assertEquals(2, onCacheCalled.get());
        assertEquals(1, onRemovalCalled.get());

        ifdService.clearField("field_2");

        assertEquals(2, onCacheCalled.get());
        assertEquals(2, onRemovalCalled.get());

        reader.close();
        loadField1.close();
        loadField2.close();
        writer.close();
        ifdService.clear();
    }

    public void testFieldDataCacheListener() throws Exception {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        // copy the ifdService since we can set the listener only once.
        final IndexFieldDataService ifdService = new IndexFieldDataService(indexService.getIndexSettings(),
                indicesService.getIndicesFieldDataCache(), indicesService.getCircuitBreakerService(), indexService.mapperService());

        final BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        final MappedFieldType mapper1 = new TextFieldMapper.Builder("s").fielddata(true).build(ctx).fieldType();
        final IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField("s", "thisisastring", Store.NO));
        writer.addDocument(doc);
        DirectoryReader open = DirectoryReader.open(writer);
        final boolean wrap = randomBoolean();
        final IndexReader reader = wrap ? ElasticsearchDirectoryReader.wrap(open, new ShardId("test", "_na_", 1)) : open;
        final AtomicInteger onCacheCalled = new AtomicInteger();
        final AtomicInteger onRemovalCalled = new AtomicInteger();
        ifdService.setListener(new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
                if (wrap) {
                    assertEquals(new ShardId("test", "_na_", 1), shardId);
                } else {
                    assertNull(shardId);
                }
                onCacheCalled.incrementAndGet();
            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                if (wrap) {
                    assertEquals(new ShardId("test", "_na_", 1), shardId);
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

    public void testJsonFields() throws IOException {
        // Set up the index service.
        IndexService indexService = createIndex("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexFieldDataService ifdService = new IndexFieldDataService(indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService(),
            indexService.mapperService());

        BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        JsonFieldMapper fieldMapper = new JsonFieldMapper.Builder("json").build(ctx);

        AtomicInteger onCacheCalled = new AtomicInteger();
        ifdService.setListener(new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
                assertEquals(fieldMapper.keyedFieldName(), fieldName);
                onCacheCalled.incrementAndGet();
            }
        });

        // Add some documents.
        Directory directory = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig(new KeywordAnalyzer());
        IndexWriter writer = new IndexWriter(directory, config);

        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("json._keyed", new BytesRef("some_key\0some_value")));
        writer.addDocument(doc);
        writer.commit();
        writer.addDocument(doc);
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(
            DirectoryReader.open(writer),
            new ShardId("test", "_na_", 1));

        // Load global field data for subfield 'key'.
        KeyedJsonFieldType fieldType1 = fieldMapper.keyedFieldType("key");
        IndexFieldData<?> ifd1 = ifdService.getForField(fieldType1);
        assertTrue(ifd1 instanceof KeyedJsonIndexFieldData);

        KeyedJsonIndexFieldData fieldData1 = (KeyedJsonIndexFieldData) ifd1;
        assertEquals("key", fieldData1.getKey());
        fieldData1.loadGlobal(reader);
        assertEquals(1, onCacheCalled.get());

        // Load global field data for the subfield 'other_key'.
        KeyedJsonFieldType fieldType2 = fieldMapper.keyedFieldType("other_key");
        IndexFieldData<?> ifd2 = ifdService.getForField(fieldType2);
        assertTrue(ifd2 instanceof KeyedJsonIndexFieldData);

        KeyedJsonIndexFieldData fieldData2 = (KeyedJsonIndexFieldData) ifd2;
        assertEquals("other_key", fieldData2.getKey());
        fieldData2.loadGlobal(reader);
        assertEquals(1, onCacheCalled.get());

        ifdService.clear();
        reader.close();
        writer.close();
        directory.close();
    }

    public void testSetCacheListenerTwice() {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexFieldDataService shardPrivateService = new IndexFieldDataService(indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(), indicesService.getCircuitBreakerService(), indexService.mapperService());
        // set it the first time...
        shardPrivateService.setListener(new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {

            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {

            }
        });
        // now set it again and make sure we fail
        try {
            shardPrivateService.setListener(new IndexFieldDataCache.Listener() {
                @Override
                public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {

                }

                @Override
                public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {

                }
            });
            fail("listener already set");
        } catch (IllegalStateException ex) {
            // all well
        }
    }

    private void doTestRequireDocValues(MappedFieldType ft) {
        ThreadPool threadPool = new TestThreadPool("random_threadpool_name");
        try {
            IndicesFieldDataCache cache = new IndicesFieldDataCache(Settings.EMPTY, null);
            IndexFieldDataService ifds =
                new IndexFieldDataService(IndexSettingsModule.newIndexSettings("test", Settings.EMPTY), cache, null, null);
            ft.setName("some_long");
            ft.setHasDocValues(true);
            ifds.getForField(ft); // no exception
            ft.setHasDocValues(false);
            try {
                ifds.getForField(ft);
                fail();
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage(), containsString("doc values"));
            }
        } finally {
            threadPool.shutdown();
        }
    }

    public void testRequireDocValuesOnLongs() {
        doTestRequireDocValues(new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG));
    }

    public void testRequireDocValuesOnDoubles() {
        doTestRequireDocValues(new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE));
    }

    public void testRequireDocValuesOnBools() {
        doTestRequireDocValues(new BooleanFieldMapper.BooleanFieldType());
    }

}
