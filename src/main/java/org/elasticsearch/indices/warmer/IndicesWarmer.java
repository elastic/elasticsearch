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

package org.elasticsearch.indices.warmer;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.threadpool.ThreadPool;

/**
 */
public interface IndicesWarmer {

    public abstract class Listener {

        public String executor() {
            return ThreadPool.Names.WARMER;
        }

        /** A handle on the execution of  warm-up action. */
        public static interface TerminationHandle {

            public static TerminationHandle NO_WAIT = new TerminationHandle() {
                @Override
                public void awaitTermination() {}
            };

            /** Wait until execution of the warm-up action completes. */
            void awaitTermination() throws InterruptedException;
        }

        /** Queue tasks to warm-up the given segments and return handles that allow to wait for termination of the execution of those tasks. */
        public abstract TerminationHandle warm(IndexShard indexShard, IndexMetaData indexMetaData, WarmerContext context, ThreadPool threadPool);

        public TerminationHandle warmTop(IndexShard indexShard, IndexMetaData indexMetaData, WarmerContext context, ThreadPool threadPool) {
            return TerminationHandle.NO_WAIT;
        }
    }

    public static class WarmerContext {

        private final ShardId shardId;

        private final Engine.Searcher newSearcher;

        private final IndexReader indexReader;

        public WarmerContext(ShardId shardId, Engine.Searcher newSearcher) {
            this.shardId = shardId;
            this.newSearcher = newSearcher;
            this.indexReader = null;
        }

        public WarmerContext(ShardId shardId, IndexReader indexReader) {
            this.shardId = shardId;
            this.newSearcher = null;
            this.indexReader = indexReader;
        }

        public ShardId shardId() {
            return shardId;
        }

        /** Return a searcher instance that only wraps the segments to warm. */
        public Engine.Searcher newSearcher() {
            return newSearcher;
        }

        public IndexReader indexReader() {
            return indexReader;
        }

        @Override
        public String toString() {
            final String value;
            if (newSearcher != null) {
                value = newSearcher.reader().toString();
            } else if (indexReader != null) {
                value = indexReader.toString();
            } else {
                value = "null";
            }
            return "WarmerContext: " + value;
        }
    }

    void addListener(Listener listener);

    void removeListener(Listener listener);
}
