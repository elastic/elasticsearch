/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.flattened.mapper;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.flattened.FlattenedMapperPlugin;
import org.elasticsearch.xpack.flattened.mapper.FlatObjectFieldMapper.KeyedFlatObjectFieldData;
import org.elasticsearch.xpack.flattened.mapper.FlatObjectFieldMapper.KeyedFlatObjectFieldType;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class FlatObjectIndexFieldDataTests extends ESSingleNodeTestCase  {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(FlattenedMapperPlugin.class, XPackPlugin.class);
    }

    public void testGlobalFieldDataCaching() throws IOException {
        // Set up the index service.
        IndexService indexService = createIndex("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexFieldDataService ifdService = new IndexFieldDataService(indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService(),
            indexService.mapperService());

        Mapper.BuilderContext ctx = new Mapper.BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        FlatObjectFieldMapper fieldMapper = new FlatObjectFieldMapper.Builder("json").build(ctx);

        AtomicInteger onCacheCalled = new AtomicInteger();
        ifdService.setListener(new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
                assertEquals(fieldMapper.keyedFieldName(), fieldName);
                onCacheCalled.incrementAndGet();
            }
        });

        // Add some documents.
        Directory directory = LuceneTestCase.newDirectory();
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
        KeyedFlatObjectFieldType fieldType1 = fieldMapper.keyedFieldType("key");
        IndexFieldData<?> ifd1 = ifdService.getForField(fieldType1);
        assertTrue(ifd1 instanceof KeyedFlatObjectFieldData);

        KeyedFlatObjectFieldData fieldData1 = (KeyedFlatObjectFieldData) ifd1;
        assertEquals("key", fieldData1.getKey());
        fieldData1.loadGlobal(reader);
        assertEquals(1, onCacheCalled.get());

        // Load global field data for the subfield 'other_key'.
        KeyedFlatObjectFieldType fieldType2 = fieldMapper.keyedFieldType("other_key");
        IndexFieldData<?> ifd2 = ifdService.getForField(fieldType2);
        assertTrue(ifd2 instanceof KeyedFlatObjectFieldData);

        KeyedFlatObjectFieldData fieldData2 = (KeyedFlatObjectFieldData) ifd2;
        assertEquals("other_key", fieldData2.getKey());
        fieldData2.loadGlobal(reader);
        assertEquals(1, onCacheCalled.get());

        ifdService.clear();
        reader.close();
        writer.close();
        directory.close();
    }
}
