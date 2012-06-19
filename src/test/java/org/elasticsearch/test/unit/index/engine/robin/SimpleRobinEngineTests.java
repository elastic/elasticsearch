/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test.unit.index.engine.robin;

import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.bloom.none.NoneBloomCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.robin.RobinEngine;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.unit.index.engine.AbstractSimpleEngineTests;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

/**
 *
 */
public class SimpleRobinEngineTests extends AbstractSimpleEngineTests {

    protected Engine createEngine(Store store, Translog translog) {
        return new RobinEngine(shardId, EMPTY_SETTINGS, threadPool, new IndexSettingsService(shardId.index(), EMPTY_SETTINGS), new ShardIndexingService(shardId, EMPTY_SETTINGS), null, store, createSnapshotDeletionPolicy(), translog, createMergePolicy(), createMergeScheduler(),
                new AnalysisService(shardId.index()), new SimilarityService(shardId.index()), new NoneBloomCache(shardId.index()));
    }
}
