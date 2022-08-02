/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.KeyedFlattenedFieldData;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class FlattenedIndexFieldDataTests extends ESSingleNodeTestCase {

    public void testGlobalFieldDataCaching() throws IOException {
        // Set up the index service.
        IndexService indexService = createIndex("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexFieldDataService ifdService = new IndexFieldDataService(
            indexService.getIndexSettings(),
            indicesService.getIndicesFieldDataCache(),
            indicesService.getCircuitBreakerService()
        );

        FlattenedFieldMapper fieldMapper = new FlattenedFieldMapper.Builder("flattened").build(MapperBuilderContext.ROOT);
        MappedFieldType fieldType1 = fieldMapper.fieldType().getChildFieldType("key");

        AtomicInteger onCacheCalled = new AtomicInteger();
        ifdService.setListener(new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
                assertEquals(fieldType1.name(), fieldName);
                onCacheCalled.incrementAndGet();
            }
        });

        // Add some documents.
        Directory directory = LuceneTestCase.newDirectory();
        IndexWriterConfig config = new IndexWriterConfig(new KeywordAnalyzer());
        IndexWriter writer = new IndexWriter(directory, config);

        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("flattened._keyed", new BytesRef("some_key\0some_value")));
        writer.addDocument(doc);
        writer.commit();
        writer.addDocument(doc);
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("test", "_na_", 1));

        // Load global field data for subfield 'key'.
        IndexFieldData<?> ifd1 = ifdService.getForField(fieldType1, FieldDataContext.noRuntimeFields("test"));
        assertTrue(ifd1 instanceof KeyedFlattenedFieldData);

        KeyedFlattenedFieldData fieldData1 = (KeyedFlattenedFieldData) ifd1;
        assertEquals("key", fieldData1.getKey());
        fieldData1.loadGlobal(reader);
        assertEquals(1, onCacheCalled.get());

        // Load global field data for the subfield 'other_key'.
        MappedFieldType fieldType2 = fieldMapper.fieldType().getChildFieldType("other_key");
        IndexFieldData<?> ifd2 = ifdService.getForField(fieldType2, FieldDataContext.noRuntimeFields("test"));
        assertTrue(ifd2 instanceof KeyedFlattenedFieldData);

        KeyedFlattenedFieldData fieldData2 = (KeyedFlattenedFieldData) ifd2;
        assertEquals("other_key", fieldData2.getKey());
        fieldData2.loadGlobal(reader);
        assertEquals(1, onCacheCalled.get());

        ifdService.clear();
        reader.close();
        writer.close();
        directory.close();
    }
}
