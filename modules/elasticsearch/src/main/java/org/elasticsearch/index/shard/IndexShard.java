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

import org.apache.lucene.index.Term;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.concurrent.ThreadSafe;

/**
 * @author kimchy (Shay Banon)
 */
@IndexShardLifecycle
@ThreadSafe
public interface IndexShard extends IndexShardComponent {

    ShardRouting routingEntry();

    IndexShardState state();

    /**
     * Returns the estimated flushable memory size. Returns <tt>null</tt> if not available.
     */
    SizeValue estimateFlushableMemorySize() throws ElasticSearchException;

    void create(String type, String id, String source) throws ElasticSearchException;

    void index(String type, String id, String source) throws ElasticSearchException;

    void delete(String type, String id);

    void delete(Term uid);

    void deleteByQuery(String querySource, @Nullable String queryParserName, String... types) throws ElasticSearchException;

    String get(String type, String id) throws ElasticSearchException;

    long count(float minScore, String querySource, @Nullable String queryParserName, String... types) throws ElasticSearchException;

    void refresh(boolean waitForOperations) throws ElasticSearchException;

    void flush() throws ElasticSearchException;

    void snapshot(Engine.SnapshotHandler snapshotHandler) throws EngineException;

    void recover(Engine.RecoveryHandler recoveryHandler) throws EngineException;

    Engine.Searcher searcher();

    void close();

    /**
     * Returns <tt>true</tt> if this shard can ignore a recovery attempt made to it (since the already doing/done it)
     */
    public boolean ignoreRecoveryAttempt();
}
