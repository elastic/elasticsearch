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

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FlatObjectFieldMapper.KeyedFlatObjectFieldData;
import org.elasticsearch.index.mapper.FlatObjectFieldMapper.KeyedFlatObjectFieldType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugin.mapper.FlattenedMapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class FlatObjectIndexFieldDataTests extends ESSingleNodeTestCase  {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(FlattenedMapperPlugin.class, InternalSettingsPlugin.class);
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
