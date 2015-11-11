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

package org.elasticsearch.index.shard;

import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.IndexSearcherWrapper;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.FieldMaskingReader;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, randomDynamicTemplates = false)
public class IndexSearcherWrapperIT extends ESIntegTestCase {

    public static class TestPlugin extends Plugin {
        @Override
        public String name() {
            return "index-searcher-wrapper-plugin";
        }
        @Override
        public String description() {
            return "a searcher wrapper for testing";
        }

        @Override
        public Collection<Module> shardModules(Settings indexSettings) {
            return Collections.singleton((Module)new AbstractModule() {
                @Override
                protected void configure() {
                    Multibinder<IndexSearcherWrapper> multibinder
                            = Multibinder.newSetBinder(binder(), IndexSearcherWrapper.class);
                    multibinder.addBinding().to(Wrapper.class);
                }
            });
        }
    }
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(TestPlugin.class);
    }

    public static final class Wrapper implements IndexSearcherWrapper {
        @Override
        public DirectoryReader wrap(DirectoryReader reader) throws IOException {
            return new FieldMaskingReader("foo", reader);
        }

        @Override
        public IndexSearcher wrap(EngineConfig engineConfig, IndexSearcher searcher) throws EngineException {
            return searcher;
        }
    }

    public void testSearcherWrapperIsUsed() throws IOException {
        prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)).get();
        ensureGreen();
        Iterable<IndicesService> indicesServices = internalCluster().getDataNodeInstances(IndicesService.class);
        Iterator<IndicesService> iterator = indicesServices.iterator();
        IndicesService indicesService = iterator.next();
        assertFalse("only one node", iterator.hasNext());
        IndexService indexService = indicesService.indexService("test");

        IndexShard shard = indexService.shard(0);
        client().prepareIndex("test", "test", "0").setSource("{\"foo\" : \"bar\"}").setRefresh(true).get();
        client().prepareIndex("test", "test", "1").setSource("{\"foobar\" : \"bar\"}").setRefresh(true).get();

        Engine.GetResult getResult = shard.get(new Engine.Get(false, new Term(UidFieldMapper.NAME, Uid.createUid("test", "1"))));
        assertTrue(getResult.exists());
        assertNotNull(getResult.searcher());
        getResult.release();
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertTrue(searcher.reader() instanceof FieldMaskingReader);
            TopDocs search = searcher.searcher().search(new TermQuery(new Term("foo", "bar")), 10);
            assertEquals(search.totalHits, 0);
            search = searcher.searcher().search(new TermQuery(new Term("foobar", "bar")), 10);
            assertEquals(search.totalHits, 1);
        }
    }

    public void testSearcherWrapperWorksWithGlobaOrdinals() {
        prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)).get();
        ensureGreen();
        Iterable<IndicesService> indicesServices = internalCluster().getDataNodeInstances(IndicesService.class);
        Iterator<IndicesService> iterator = indicesServices.iterator();
        IndicesService indicesService = iterator.next();
        assertFalse("only one node", iterator.hasNext());
        IndexService indexService = indicesService.indexService("test");

        IndexShard shard = indexService.shard(0);
        client().prepareIndex("test", "test", "0").setSource("{\"foo\" : \"bar\"}").setRefresh(true).get();
        client().prepareIndex("test", "test", "1").setSource("{\"foobar\" : \"bar\"}").setRefresh(true).get();

        // test global ordinals are evicted
        MappedFieldType foo = shard.mapperService().indexName("foo");
        IndexFieldData.Global ifd = shard.indexFieldDataService().getForField(foo);
        FieldDataStats before = shard.fieldData().stats("foo");
        assertThat(before.getMemorySizeInBytes(), equalTo(0l));
        FieldDataStats after = null;
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertTrue(searcher.reader() instanceof FieldMaskingReader);
            assumeTrue("we have to have more than one segment", searcher.getDirectoryReader().leaves().size() > 1);
            IndexFieldData indexFieldData = ifd.loadGlobal(searcher.getDirectoryReader());
            after = shard.fieldData().stats("foo");
            assertEquals(after.getEvictions(), before.getEvictions());
            // If a field doesn't exist an empty IndexFieldData is returned and that isn't cached:
            assertThat(after.getMemorySizeInBytes(), equalTo(0l));
        }
        assertEquals(shard.fieldData().stats("foo").getEvictions(), before.getEvictions());
        assertEquals(shard.fieldData().stats("foo").getMemorySizeInBytes(), after.getMemorySizeInBytes());
        shard.flush(new FlushRequest().force(true).waitIfOngoing(true));
        shard.refresh("test");
        assertEquals(shard.fieldData().stats("foo").getMemorySizeInBytes(), before.getMemorySizeInBytes());
    }

}
