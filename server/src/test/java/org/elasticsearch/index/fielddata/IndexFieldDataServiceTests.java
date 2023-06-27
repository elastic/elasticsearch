/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.plain.SortedDoublesIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.ArgumentMatchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.index.mapper.NumberFieldMapper.NumberType.BYTE;
import static org.elasticsearch.index.mapper.NumberFieldMapper.NumberType.DOUBLE;
import static org.elasticsearch.index.mapper.NumberFieldMapper.NumberType.INTEGER;
import static org.elasticsearch.index.mapper.NumberFieldMapper.NumberType.LONG;
import static org.elasticsearch.index.mapper.NumberFieldMapper.NumberType.SHORT;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexFieldDataServiceTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testGetForFieldDefaults() {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexFieldDataService ifdService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService()
        );
        MapperBuilderContext context = MapperBuilderContext.root(false);
        final MappedFieldType stringMapper = new KeywordFieldMapper.Builder("string", IndexVersion.current()).build(context).fieldType();
        ifdService.clear();
        IndexFieldData<?> fd = ifdService.getForField(stringMapper, FieldDataContext.noRuntimeFields("test"));
        assertTrue(fd instanceof SortedSetOrdinalsIndexFieldData);

        for (MappedFieldType mapper : Arrays.asList(
            new NumberFieldMapper.Builder("int", BYTE, ScriptCompiler.NONE, false, true, IndexVersion.current(), null).build(context)
                .fieldType(),
            new NumberFieldMapper.Builder("int", SHORT, ScriptCompiler.NONE, false, true, IndexVersion.current(), null).build(context)
                .fieldType(),
            new NumberFieldMapper.Builder("int", INTEGER, ScriptCompiler.NONE, false, true, IndexVersion.current(), null).build(context)
                .fieldType(),
            new NumberFieldMapper.Builder("long", LONG, ScriptCompiler.NONE, false, true, IndexVersion.current(), null).build(context)
                .fieldType()
        )) {
            ifdService.clear();
            fd = ifdService.getForField(mapper, FieldDataContext.noRuntimeFields("test"));
            assertTrue(fd instanceof SortedNumericIndexFieldData);
        }

        final MappedFieldType floatMapper = new NumberFieldMapper.Builder(
            "float",
            NumberType.FLOAT,
            ScriptCompiler.NONE,
            false,
            true,
            IndexVersion.current(),
            null
        ).build(context).fieldType();
        ifdService.clear();
        fd = ifdService.getForField(floatMapper, FieldDataContext.noRuntimeFields("test"));
        assertTrue(fd instanceof SortedDoublesIndexFieldData);

        final MappedFieldType doubleMapper = new NumberFieldMapper.Builder(
            "double",
            DOUBLE,
            ScriptCompiler.NONE,
            false,
            true,
            IndexVersion.current(),
            null
        ).build(context).fieldType();
        ifdService.clear();
        fd = ifdService.getForField(doubleMapper, FieldDataContext.noRuntimeFields("test"));
        assertTrue(fd instanceof SortedDoublesIndexFieldData);
    }

    public void testGetForFieldRuntimeField() {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexFieldDataService ifdService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService()
        );
        final SetOnce<Supplier<SearchLookup>> searchLookupSetOnce = new SetOnce<>();
        MappedFieldType ft = mock(MappedFieldType.class);
        when(ft.fielddataBuilder(ArgumentMatchers.any())).thenAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            FieldDataContext fdc = (FieldDataContext) invocationOnMock.getArguments()[0];
            searchLookupSetOnce.set(fdc.lookupSupplier());
            return (IndexFieldData.Builder) (cache, breakerService) -> null;
        });
        SearchLookup searchLookup = new SearchLookup(null, null, (ctx, doc) -> null);
        ifdService.getForField(ft, new FieldDataContext("qualified", () -> searchLookup, null, MappedFieldType.FielddataOperation.SEARCH));
        assertSame(searchLookup, searchLookupSetOnce.get().get());
    }

    public void testClearField() throws Exception {
        final IndexService indexService = createIndex("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        // copy the ifdService since we can set the listener only once.
        final IndexFieldDataService ifdService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService()
        );

        final MapperBuilderContext context = MapperBuilderContext.root(false);
        final MappedFieldType mapper1 = new TextFieldMapper.Builder("field_1", createDefaultIndexAnalyzers()).fielddata(true)
            .build(context)
            .fieldType();
        final MappedFieldType mapper2 = new TextFieldMapper.Builder("field_2", createDefaultIndexAnalyzers()).fielddata(true)
            .build(context)
            .fieldType();
        final IndexWriter writer = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
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
        IndexFieldData<?> ifd1 = ifdService.getForField(mapper1, FieldDataContext.noRuntimeFields("test"));
        IndexFieldData<?> ifd2 = ifdService.getForField(mapper2, FieldDataContext.noRuntimeFields("test"));
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        LeafFieldData loadField1 = ifd1.load(leafReaderContext);
        LeafFieldData loadField2 = ifd2.load(leafReaderContext);

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
        final IndexFieldDataService ifdService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService()
        );

        final MapperBuilderContext context = MapperBuilderContext.root(false);
        final MappedFieldType mapper1 = new TextFieldMapper.Builder("s", createDefaultIndexAnalyzers()).fielddata(true)
            .build(context)
            .fieldType();
        final IndexWriter writer = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
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
        IndexFieldData<?> ifd = ifdService.getForField(mapper1, FieldDataContext.noRuntimeFields("test"));
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        LeafFieldData load = ifd.load(leafReaderContext);
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
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexFieldDataService shardPrivateService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService()
        );
        // set it the first time...
        shardPrivateService.setListener(new IndexFieldDataCache.Listener() {

        });
        // now set it again and make sure we fail
        try {
            shardPrivateService.setListener(new IndexFieldDataCache.Listener() {

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
            IndexFieldDataService ifds = new IndexFieldDataService(
                IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
                cache,
                null
            );
            if (ft.hasDocValues()) {
                ifds.getForField(ft, FieldDataContext.noRuntimeFields("test")); // no exception
            } else {
                IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> ifds.getForField(ft, FieldDataContext.noRuntimeFields("test"))
                );
                assertThat(e.getMessage(), containsString("doc values"));
            }
        } finally {
            threadPool.shutdown();
        }
    }

    public void testRequireDocValuesOnLongs() {
        doTestRequireDocValues(new NumberFieldMapper.NumberFieldType("field", LONG));
        doTestRequireDocValues(
            new NumberFieldMapper.NumberFieldType(
                "field",
                LONG,
                true,
                false,
                false,
                false,
                null,
                Collections.emptyMap(),
                null,
                false,
                null,
                null
            )
        );
    }

    public void testRequireDocValuesOnDoubles() {
        doTestRequireDocValues(new NumberFieldMapper.NumberFieldType("field", NumberType.DOUBLE));
        doTestRequireDocValues(
            new NumberFieldMapper.NumberFieldType(
                "field",
                NumberType.DOUBLE,
                true,
                false,
                false,
                false,
                null,
                Collections.emptyMap(),
                null,
                false,
                null,
                null
            )
        );
    }

    public void testRequireDocValuesOnBools() {
        doTestRequireDocValues(new BooleanFieldMapper.BooleanFieldType("field"));
        doTestRequireDocValues(new BooleanFieldMapper.BooleanFieldType("field", true, false, false, null, null, Collections.emptyMap()));
    }
}
