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
package org.elasticsearch.indices.leaks;

import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.ESIntegTestCase.Scope;
import static org.hamcrest.Matchers.nullValue;

/**
 */
@ClusterScope(scope= Scope.TEST, numDataNodes =1)
public class IndicesLeaksIT extends ESIntegTestCase {


    @SuppressWarnings({"ConstantConditions", "unchecked"})
    @Test
    @BadApple(bugUrl = "https://github.com/elasticsearch/elasticsearch/issues/3232")
    public void testIndexShardLifecycleLeak() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();

        client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        IndicesService indicesService = internalCluster().getDataNodeInstance(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe("test");
        Injector indexInjector = indexService.injector();
        IndexShard shard = indexService.shardSafe(0);
        Injector shardInjector = indexService.shardInjectorSafe(0);

        performCommonOperations();

        List<WeakReference> indexReferences = new ArrayList<>();
        List<WeakReference> shardReferences = new ArrayList<>();

        // TODO if we could iterate over the already created classes on the injector, we can just add them here to the list
        // for now, we simple add some classes that make sense

        // add index references
        indexReferences.add(new WeakReference(indexService));
        indexReferences.add(new WeakReference(indexInjector));
        indexReferences.add(new WeakReference(indexService.mapperService()));
        for (DocumentMapper documentMapper : indexService.mapperService().docMappers(true)) {
            indexReferences.add(new WeakReference(documentMapper));
        }
        indexReferences.add(new WeakReference(indexService.aliasesService()));
        indexReferences.add(new WeakReference(indexService.analysisService()));
        indexReferences.add(new WeakReference(indexService.fieldData()));
        indexReferences.add(new WeakReference(indexService.queryParserService()));


        // add shard references
        shardReferences.add(new WeakReference(shard));
        shardReferences.add(new WeakReference(shardInjector));

        indexService = null;
        indexInjector = null;
        shard = null;
        shardInjector = null;

        cluster().wipeIndices("test");

        for (int i = 0; i < 100; i++) {
            System.gc();
            int indexNotCleared = 0;
            for (WeakReference indexReference : indexReferences) {
                if (indexReference.get() != null) {
                    indexNotCleared++;
                }
            }
            int shardNotCleared = 0;
            for (WeakReference shardReference : shardReferences) {
                if (shardReference.get() != null) {
                    shardNotCleared++;
                }
            }
            logger.info("round {}, indices {}/{}, shards {}/{}", i, indexNotCleared, indexReferences.size(), shardNotCleared, shardReferences.size());
            if (indexNotCleared == 0 && shardNotCleared == 0) {
                break;
            }
        }

        //System.out.println("sleeping");Thread.sleep(1000000);

        for (WeakReference indexReference : indexReferences) {
            assertThat("dangling index reference: " + indexReference.get(), indexReference.get(), nullValue());
        }

        for (WeakReference shardReference : shardReferences) {
            assertThat("dangling shard reference: " + shardReference.get(), shardReference.get(), nullValue());
        }
    }

    private void performCommonOperations() {
        client().prepareIndex("test", "type", "1").setSource("field1", "value", "field2", 2, "field3", 3.0f).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        client().prepareSearch("test").setQuery(QueryBuilders.queryStringQuery("field1:value")).execute().actionGet();
        client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field1", "value")).execute().actionGet();
    }
}
