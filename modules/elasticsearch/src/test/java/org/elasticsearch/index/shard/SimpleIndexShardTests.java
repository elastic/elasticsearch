/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.cache.filter.none.NoneFilterCache;
import org.elasticsearch.index.deletionpolicy.KeepOnlyLastDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.robin.RobinEngine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.merge.policy.LogByteSizeMergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.SerialMergeSchedulerProvider;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.ram.RamStore;
import org.elasticsearch.index.translog.memory.MemoryTranslog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.dynamic.DynamicThreadPool;
import org.elasticsearch.util.settings.Settings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy
 */
public class SimpleIndexShardTests {

    private ThreadPool threadPool;

    private IndexShard indexShard;

    @BeforeMethod public void createIndexShard() {
        Settings settings = EMPTY_SETTINGS;
        Environment environment = new Environment(settings);
        ShardId shardId = new ShardId("test", 1);
        AnalysisService analysisService = new AnalysisService(shardId.index());
        MapperService mapperService = new MapperService(shardId.index(), settings, environment, analysisService);
        IndexQueryParserService queryParserService = new IndexQueryParserService(shardId.index(), mapperService, new NoneFilterCache(shardId.index(), EMPTY_SETTINGS), analysisService);
        FilterCache filterCache = new NoneFilterCache(shardId.index(), settings);

        SnapshotDeletionPolicy policy = new SnapshotDeletionPolicy(new KeepOnlyLastDeletionPolicy(shardId, settings));
        Store store = new RamStore(shardId, settings);
        MemoryTranslog translog = new MemoryTranslog(shardId, settings);
        Engine engine = new RobinEngine(shardId, settings, store, policy, translog,
                new LogByteSizeMergePolicyProvider(store), new SerialMergeSchedulerProvider(shardId, settings),
                analysisService, new SimilarityService(shardId.index()));

        threadPool = new DynamicThreadPool();

        indexShard = new InternalIndexShard(shardId, EMPTY_SETTINGS, store, engine, translog, threadPool, mapperService, queryParserService, filterCache).start();
    }

    @AfterMethod public void tearDown() {
        indexShard.close();
        threadPool.shutdown();
    }

    @Test public void testSimpleIndexGetDelete() {
        String source1 = "{ type1 : { _id : \"1\", name : \"test\", age : 35 } }";
        indexShard.index("type1", "1", source1);
        indexShard.refresh(true);

        String sourceFetched = indexShard.get("type1", "1");

        assertThat(sourceFetched, equalTo(source1));

        assertThat(indexShard.count(0, "{ term : { age : 35 } }", null), equalTo(1l));
        assertThat(indexShard.count(0, "{ queryString : { query : \"name:test\" } }", null), equalTo(1l));
        assertThat(indexShard.count(0, "{ queryString : { query : \"age:35\" } }", null), equalTo(1l));

        indexShard.delete("type1", "1");
        indexShard.refresh(true);

        assertThat(indexShard.get("type1", "1"), nullValue());

        indexShard.index("type1", "1", source1);
        indexShard.refresh(true);
        sourceFetched = indexShard.get("type1", "1");
        assertThat(sourceFetched, equalTo(source1));
        indexShard.deleteByQuery("{ term : { name : \"test\" } }", null);
        indexShard.refresh(true);
        assertThat(indexShard.get("type1", "1"), nullValue());

        indexShard.close();
    }
}
