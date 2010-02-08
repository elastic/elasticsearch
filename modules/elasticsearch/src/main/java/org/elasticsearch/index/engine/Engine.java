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

package org.elasticsearch.index.engine;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.IndexShardLifecycle;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.concurrent.ThreadSafe;
import org.elasticsearch.util.lease.Releasable;

/**
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
@IndexShardLifecycle
public interface Engine extends IndexShardComponent {

    /**
     * Starts the Engine.
     *
     * <p>Note, after the creation and before the call to start, the store might
     * be changed.
     */
    void start() throws EngineException;

    void create(Create create) throws EngineException;

    void index(Index index) throws EngineException;

    void delete(Delete delete) throws EngineException;

    void delete(DeleteByQuery delete) throws EngineException;

    Searcher searcher() throws EngineException;

    /**
     * Refreshes the engine for new search operations to reflect the latest
     * changes. Pass <tt>true</tt> if the refresh operation should include
     * all the operations performed up to this call.
     */
    void refresh(boolean waitForOperations) throws EngineException;

    /**
     * Flushes the state of the engine, clearing memory.
     */
    void flush() throws EngineException, FlushNotAllowedEngineException;

    void snapshot(SnapshotHandler snapshotHandler) throws EngineException;

    void recover(RecoveryHandler recoveryHandler) throws EngineException;

    /**
     * Returns the estimated flushable memory size. Returns <tt>null</tt> if not available.
     */
    SizeValue estimateFlushableMemorySize();

    void close() throws ElasticSearchException;

    /**
     * Recovery allow to start the recovery process. It is built of three phases.
     *
     * <p>The first phase allows to take a snapshot of the master index. Once this
     * is taken, no commit operations are effectively allowed on the index until the recovery
     * phases are through.
     *
     * <p>The seconds phase takes a snapshot of the current transaction log.
     *
     * <p>The last phase returns the remaining transaction log. During this phase, no dirty
     * operations are allowed on the index.
     */
    static interface RecoveryHandler {

        void phase1(SnapshotIndexCommit snapshot) throws ElasticSearchException;

        void phase2(Translog.Snapshot snapshot) throws ElasticSearchException;

        void phase3(Translog.Snapshot snapshot) throws ElasticSearchException;
    }

    /**
     */
    static interface SnapshotHandler {

        void snapshot(SnapshotIndexCommit snapshotIndexCommit, Translog.Snapshot translogSnapshot) throws EngineException;
    }

    static interface Searcher extends Releasable {

        IndexReader reader();

        IndexSearcher searcher();
    }

    static class Create {
        private final Document document;
        private final Analyzer analyzer;
        private final String type;
        private final String id;
        private final String source;

        public Create(Document document, Analyzer analyzer, String type, String id, String source) {
            this.document = document;
            this.analyzer = analyzer;
            this.type = type;
            this.id = id;
            this.source = source;
        }

        public String type() {
            return this.type;
        }

        public String id() {
            return this.id;
        }

        public Document doc() {
            return this.document;
        }

        public Analyzer analyzer() {
            return this.analyzer;
        }

        public String source() {
            return this.source;
        }
    }

    static class Index {
        private final Term uid;
        private final Document document;
        private final Analyzer analyzer;
        private final String type;
        private final String id;
        private final String source;

        public Index(Term uid, Document document, Analyzer analyzer, String type, String id, String source) {
            this.uid = uid;
            this.document = document;
            this.analyzer = analyzer;
            this.type = type;
            this.id = id;
            this.source = source;
        }

        public Term uid() {
            return this.uid;
        }

        public Document doc() {
            return this.document;
        }

        public Analyzer analyzer() {
            return this.analyzer;
        }

        public String id() {
            return this.id;
        }

        public String type() {
            return this.type;
        }

        public String source() {
            return this.source;
        }
    }

    static class Delete {
        private final Term uid;

        public Delete(Term uid) {
            this.uid = uid;
        }

        public Term uid() {
            return this.uid;
        }
    }

    static class DeleteByQuery {
        private final Query query;
        private final String queryParserName;
        private final String source;
        private final String[] types;

        public DeleteByQuery(Query query, String source, @Nullable String queryParserName, String... types) {
            this.query = query;
            this.source = source;
            this.queryParserName = queryParserName;
            this.types = types;
        }

        public String queryParserName() {
            return this.queryParserName;
        }

        public Query query() {
            return this.query;
        }

        public String source() {
            return this.source;
        }

        public String[] types() {
            return this.types;
        }
    }
}
