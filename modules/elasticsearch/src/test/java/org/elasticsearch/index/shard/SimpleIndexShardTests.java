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

import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.deletionpolicy.KeepOnlyLastDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.robin.RobinEngine;
import org.elasticsearch.index.engine.robin.RobinIndexEngine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.merge.policy.LogByteSizeMergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.SerialMergeSchedulerProvider;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.ram.RamStore;
import org.elasticsearch.index.translog.memory.MemoryTranslog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.scaling.ScalingThreadPool;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy
 */
public class SimpleIndexShardTests {

    private ThreadPool threadPool;

    private IndexShard indexShard;

    @BeforeMethod public void createIndexShard() throws IOException {
        Settings settings = EMPTY_SETTINGS;
        Environment environment = new Environment(settings);
        ShardId shardId = new ShardId("test", 1);
        AnalysisService analysisService = new AnalysisService(shardId.index());
        MapperService mapperService = new MapperService(shardId.index(), settings, environment, analysisService);
        IndexQueryParserService queryParserService = new IndexQueryParserService(shardId.index(), mapperService, new IndexCache(shardId.index()), new RobinIndexEngine(shardId.index()), analysisService);
        IndexCache indexCache = new IndexCache(shardId.index());

        SnapshotDeletionPolicy policy = new SnapshotDeletionPolicy(new KeepOnlyLastDeletionPolicy(shardId, settings));
        Store store = new RamStore(shardId, settings);
        MemoryTranslog translog = new MemoryTranslog(shardId, settings);
        Engine engine = new RobinEngine(shardId, settings, store, policy, translog,
                new LogByteSizeMergePolicyProvider(store), new SerialMergeSchedulerProvider(shardId, settings),
                analysisService, new SimilarityService(shardId.index()));

        threadPool = new ScalingThreadPool();

        indexShard = new InternalIndexShard(shardId, EMPTY_SETTINGS, store, engine, translog, threadPool, mapperService, queryParserService, indexCache).start();
    }

    @AfterMethod public void tearDown() {
        indexShard.close();
        threadPool.shutdown();
    }

    @Test public void testSimpleIndexGetDelete() {
        String source1 = "{ type1 : { _id : \"1\", name : \"test\", age : 35 } }";
        indexShard.index("type1", "1", Unicode.fromStringAsBytes(source1));
        indexShard.refresh(new Engine.Refresh(true));

        String sourceFetched = Unicode.fromBytes(indexShard.get("type1", "1"));

        assertThat(sourceFetched, equalTo(source1));

        assertThat(indexShard.count(0, termQuery("age", 35).buildAsBytes(), null), equalTo(1l));
        assertThat(indexShard.count(0, queryString("name:test").buildAsBytes(), null), equalTo(1l));
        assertThat(indexShard.count(0, queryString("age:35").buildAsBytes(), null), equalTo(1l));

        indexShard.delete("type1", "1");
        indexShard.refresh(new Engine.Refresh(true));

        assertThat(indexShard.get("type1", "1"), nullValue());

        indexShard.index("type1", "1", Unicode.fromStringAsBytes(source1));
        indexShard.refresh(new Engine.Refresh(true));
        sourceFetched = Unicode.fromBytes(indexShard.get("type1", "1"));
        assertThat(sourceFetched, equalTo(source1));
        indexShard.deleteByQuery(termQuery("name", "test").buildAsBytes(), null);
        indexShard.refresh(new Engine.Refresh(true));
        assertThat(indexShard.get("type1", "1"), nullValue());

        indexShard.close();
    }
}
